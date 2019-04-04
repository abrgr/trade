(ns com.adamgberger.predictit.lib.observable-state
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s])
  (:gen-class))

(defn- validate
    [producer-cfg-map state]
    (every?
        (fn [[keypath {:keys [validator]}]]
            (or (nil? validator)
                (validator (get-in state keypath))))
        state))

(defn- with-merge-meta [obj m]
  (with-meta obj (merge (meta obj) m)))

(defn- on-result [update-path send-update new-state prev keypath {:keys [validator] :as cfg} result]
    (let [valid? (or (nil? validator)
                     (validator result))
          effective-result (if valid?
                               result
                               (with-merge-meta prev {::invalid-value result}))
          new-val (with-merge-meta effective-result {::updated-at (java.time.Instant/now)})]
        (send-update update-path keypath (assoc new-state keypath new-val))))

(defn- handle-updates [send-update
                       keypath
                       {:keys [param-keypaths io-producer compute-producer projection] :as cfg}
                       updates]
    (let [new-states (map :new-state updates)
          new-state (apply merge new-states)
          partial-state (select-keys new-state param-keypaths)
          prev (keypath new-state)
          args {:partial-state partial-state
                :prev prev
                :key keypath}
          update-path (if (= (count updates) 1)
                        (-> updates first :update-path)
                        (map :update-path updates)) ; TODO: this seems wrong
          handle-result (partial on-result update-path send-update new-state prev keypath cfg)]
        (cond
            ; TODO: timeouts
            (some? io-producer) (async/go (io-producer args handle-result))
            (some? compute-producer) (async/go (let [res (async/thread (compute-producer args))]
                                                    (handle-result (async/<! res))))
            (some? projection) (async/go (let [res (async/thread (projection args))]
                                                    (handle-result (async/<! res)))))))
(defn- register-periodicity [start-txn keypath update-pub {:keys [at-least-every-ms jitter-pct] :or {jitter-pct 0}}]
    (when (some? periodicity)
        (let [periodicity-ch (async/chan)
              periodicity-pub (async/pub periodicity-ch (constantly ::periodically))
              keypath-change-ch (async/chan)]
            (async/go-loop []
                ; idea: loop here waiting for our keypath to produce something OR for the timeout to elapse
                ; if the timeout elapses, publish an "update" to our pub
                ; have the producer subscribe to the pub we return (if we return one)
                ; ACTUALLY... this needs to publish to a transaction creation queue so that we ensure we only have 1 txn 
                ; in flight at a time.
                (let [[update port] (async/alts! [keypath-change-ch
                                                  (async/timeout (* at-least-every-ms (+ 1 (* (rand) jitter-pct))))])]
                    (when (not= port keypath-change-ch)
                      (start-txn :periodicity keypath)))))))

(defn- register-producer [logger
                          state-atom
                          descendants
                          all-ancestors
                          start-txn
                          send-update
                          pub
                          keypath
                          {:keys [param-keypaths periodicity] :as cfg}]
    (let [update-ch (async/chan)
          param-ancestors (->> param-keypaths
                               (mapcat all-ancestors)
                               (into #{}))
          uniq-param-keypaths (->> param-keypaths
                                   (filter (comp not param-ancestors))
                                   (into #{}))]
        (async/go-loop []
            (when-let [update (async/<! update-ch)]
                ; based on the origin-keypath in update, we find all of the params of ours that we expect to be fired.
                ; we wait for all of them before running
                (let [{updated-keypath :keypath :keys [txn-id orig-keypath]} update
                      orig-descendants (descendants orig-keypath)
                      ; note: required-params is empty when (= updated-keypath keypath) by non-cyclicality
                      required-params (clojure.set/intersection orig-descendants uniq-param-keypaths)
                      needed-params (disj required-params updated-keypath)
                      ; TODO: should definitely have a transaction timeout
                      {:keys [remaining updates]}
                            (reduce
                                (fn [{:keys [remaining updates]} _]
                                    (let [{next-txn-id :txn-id
                                           next-keypath :keypath
                                           :as next-update} (async/<! update-ch)]
                                        (if (or (not= next-txn-id txn-id)
                                                (not (contains? remaining next-keypath)))
                                            (do (logger :error "Mixed transactions" {:txn-id txn-id :next-txn-id next-txn-id})
                                                (reduced {:remaining #{}
                                                          :updates []}))
                                            {:remaining (disj remaining next-keypath)
                                             :updates (conj updates next-update)})))
                                {:remaining needed-params
                                 :updates [update]}
                                needed-params)]
                    (when-not (empty? remaining)
                        (logger :error "Remaining updates not empty" {:remaining remaining
                                                               :updates updates
                                                               :needed-params needed-params
                                                               :required-params required-params}))
                    (do (handle-updates send-update keypath cfg updates)
                        (recur)))))
        (when-let [periodicity-pub (register-periodicity start-txn keypath pub periodicity)]
            (async/sub periodicity-pub ::periodically update-ch))
        (doseq [param-keypath uniq-param-keypaths]
            (async/sub pub param-keypath update-ch))
        (async/sub pub keypath update-ch)))

(defn- closure-for-node [closure-by-node nexts-by-node is-cyclical? node]
    (when (is-cyclical? node) (throw (ex-info "Cycle detected" {:node node :cycle is-cyclical?})))
    (if (some? (get closure-by-node node))
        closure-by-node
        (let [next-is-cyclical? (conj is-cyclical? node)
              nexts (get nexts-by-node node)
              closure-by-node-with-nexts (reduce
                                            #(merge %1 (closure-for-node %1 nexts-by-node next-is-cyclical? %2))
                                            closure-by-node
                                            nexts)
              transitive-nexts (->> nexts
                                    (mapcat closure-by-node-with-nexts))]
            (assoc
                closure-by-node-with-nexts
                node
                (->> nexts
                     (concat transitive-nexts)
                     (into #{}))))))

(defn- transitive-closure
    ([nexts-by-node]
        (reduce
            #(merge %1 (closure-for-node %1 nexts-by-node #{} %2))
            {}
            (keys nexts-by-node)))
    ([nodes next-nodes]
        (->> nodes
             (map #(vector % (next-nodes %)))
             (into {})
             transitive-closure)))

(defn- ancestors-for-all [nodes]
    (let [deps-by-task (->> nodes
                            (group-by :task)
                            (map (fn [[k [{:keys [depends-on]}]]] [k depends-on]))
                            (into {}))]
        (transitive-closure deps-by-task)))

(defn- topo-sort
    ([nodes]
        (let [deps-by-task (->> nodes
                                (group-by :task)
                                (map (fn [[k [{:keys [depends-on]}]]] [k depends-on]))
                                (into {}))]
            (reduce
                (fn [in-order node]
                    (concat
                        in-order
                        (topo-sort (:task node) deps-by-task (into #{} in-order) #{})))
                []
                nodes)))
    ([task deps-by-task visited? is-ancestor?]
        (when (is-ancestor? task) (throw (ex-info "Circular dependency" {:task task :ancestors is-ancestor?})))
        (if (visited? task)
            nil
            (let [ancestors (conj is-ancestor? task)
                  {:keys [in-order]} (reduce
                                        (fn [{:keys [visited? in-order] :as cur} dep]
                                            (let [child-order (topo-sort dep deps-by-task visited? ancestors)]
                                                {:visited? (->> child-order
                                                                (concat visited?)
                                                                (into #{}))
                                                :in-order (->> child-order
                                                            (concat in-order))}))
                                        {:visited? visited?
                                        :in-order []}
                                        (get deps-by-task task))]
                (concat in-order [task])))))

(defn observable-state
    "Constructs an observable state with the given initial state (a map)
     and the map from keypath to producer config.
     Producer config must provide either:
      - an :io-producer (function receiving a partial state map, current value, key, and a callback function,
                         expected to return nothing and pass a value or throwable to the callback;
                         state is updated to passed value)
      - a :compute-producer (function receiving a partial state map, current value, and key and expected to return
                             a value or throw; state is updated to returned value)
      - a :projection (function receiving a partial state map, current value, and key and expected to return
                             a value or throw; state is not updated but value is propagated
     The config may provide:
      - a :validator that checks the values produced by the producer.  If the validator
        returns false, the value is rejected and the state is not updated.
      - :param-keypaths that list the keypaths that should be selected from the state when
        collecting the partial-state to pass to the producer
      - :param-keypath-validators that map keypaths from :param-keypaths to a validator function.
        If the validator function returns false, the producer will not be invoked.
      - :periodicity specifying :at-least-every-ms and an optional :jitter-pct.  The producer
        will be invoked at-least-every-ms, regardless of state changes.
     Cfg may specify:
      - a :logger that is a function of level (:info, :warn, :error), msg (string), and param map ({})."
    ([init producer-cfg-by-keypath & {:keys [logger] :or {logger prn}}]
        (let [state (atom init)
              updates-chan (async/chan)
              txn-chan (async/chan)
              p (async/pub updates-chan :keypath)
              all-ancestors (ancestors-for-all (->> producer-cfg-by-keypath
                                                    (map (fn [[keypath {:keys [param-keypaths]}]]
                                                            {:task keypath
                                                             :depends-on param-keypaths}))
                                                    (into {})))
              next-nodes (->> producer-cfg-by-keypath
                              (reduce
                                (fn [descendants [keypath {:keys [param-keypaths]}]]
                                    (->> param-keypaths
                                         (reduce
                                            (fn [descs param]
                                                (update descs param #(conj %1 keypath)))
                                            descendants)))
                                {}))
              descendants (transitive-closure next-nodes)
              terminal-node? (->> producer-cfg-by-keypath
                                  keys
                                  (filter #(= (count (next-nodes %)) 0))
                                  (into #{}))
              projections (->> producer-cfg-by-keypath
                               keys
                               (filter #(some? (get-in producer-cfg-by-keypath [% :projection])))
                               (into #{}))
              start-txn (fn [source keypath]
                          (async/go (async/>! txn-chan {:action :start
                                                        :update-path []
                                                        :txn-id (java.util.UUID/randomUUID)
                                                        :origin-keypath keypath
                                                        :keypath keypath
                                                        :txn-source source
                                                        :new-state @state})))
              send-update (fn [{:keys [update-path txn-id origin-keypath] :as update} keypath new-state]
                            (async/go (if (terminal-node? keypath)
                                        (async/>! txn-chan (assoc update :action :end))
                                        (async/>! updates-chan {:update-path (conj update-path keypath)
                                                                :txn-id txn-id
                                                                :origin-keypath origin-keypath
                                                                :keypath keypath
                                                                :new-state new-state}))))]
            (async/go-loop []
              (when-let [{:keys [txn-id action] :as txn} (async/<! txn-chan)]
                (if (not= action :start)
                  (logger :error "Expected start transaction" {:txn txn})
                  (do
                    (async/>! updates-chan (select-keys txn [:update-path :txn-id :origin-keypath :keypath :new-state]))
                    (when-let [{end-txn-id :txn-id end-action :action :as end-txn} (async/<! txn-chan)]
                      (if (not (and (= end-txn-id txn-id)
                                    (= end-action :end)))
                        (logger :error "Expected end transaction" {:start-txn txn :end-txn end-txn})
                        (send state #(merge % (apply dissoc new-state projections)))))))
                (recur)))
            (doseq [[keypath cfg] producer-cfg-by-keypath]
                (register-producer logger state descendants all-ancestors start-txn send-update p keypath cfg))
            state)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SPECS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::pred
    (s/fspec :args (s/cat :val any?)
             :ret boolean?))

(s/def ::validator ::pred)

(s/def ::result-cb
    (s/fspec :args (s/cat :val any?)
             :ret nil?))

(s/def ::partial-state map?)

(s/def ::prev any?)

(s/def ::key keyword?)

(s/def ::producer-args
    (s/keys :req-un [::partial-state ::prev ::key]))

(s/def ::io-producer
    (s/fspec :args (s/cat :producer-args ::producer-args
                          :result-cb ::result-cb)
             :ret nil?))

(s/def ::compute-producer
    (s/fspec :args (s/cat :producer-args ::producer-args)
             :ret any?))

(s/def ::projection
    (s/fspec :args (s/cat :producer-args ::producer-args)
             :ret any?))

(s/def ::at-least-every-ms int?)

(s/def ::jitter-pct double?)

(s/def ::periodicity
    (s/keys :req-un [::at-least-every-ms]
            :opt-un [::jitter-pct]))

(s/def ::keypath (s/coll-of keyword?))

(s/def ::param-keypaths (s/coll-of ::keypath))

(s/def ::param-keypath-validators (s/map-of ::keypath ::pred))

(s/def ::producer-cfg
    (s/keys :req-un [(or ::io-producer ::compute-producer ::projection)]
            :opt-un [::validator ::param-keypath-validators ::periodicity ::param-keypaths]))

(s/def ::observable-state any?)

(s/fdef observable-state
    :args (s/cat :init map?
                 :producer-cfg (s/map-of ::keypath ::producer-cfg))
    :ret ::observable-state)