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
  (if (nil? obj)
    nil
    (vary-meta obj #(merge % m))))

(defn- on-result [upd logger update-path send-update new-state prev keypath {:keys [validator] :as cfg} result]
  ; TODO: handle case where result is a throwable
  (logger :info "Finished producer" {:cfg cfg :result result})
  (let [valid? (or (nil? validator)
                   (validator result))
        effective-result (if valid?
                           result
                           (if (nil? prev)
                             nil
                             (with-merge-meta prev {::invalid-value result})))
        new-val (with-merge-meta effective-result {::updated-at (java.time.Instant/now)
                                                   ::prev (if (nil? prev)
                                                            nil
                                                            (vary-meta prev #(dissoc % ::prev)))})
        upd' (assoc upd :update-path update-path)]
    (send-update upd' keypath (assoc new-state keypath new-val))))

(defn- handle-updates [logger
                       send-update
                       keypath
                       {:keys [param-keypaths
                               transient-param-keypaths
                               io-producer
                               compute-producer
                               projection] :as cfg}
                       updates]
  (let [new-states (map :new-state updates)
        new-state (apply merge new-states)
        partial-state (select-keys new-state (concat param-keypaths transient-param-keypaths))
        prev (keypath new-state)
        args {:partial-state partial-state
              :prev prev
              :key keypath}
        update-path (if (= (count updates) 1)
                      (-> updates first :update-path)
                      (map :update-path updates)) ; TODO: this seems wrong
        handle-result (partial on-result (last updates) logger update-path send-update new-state prev keypath cfg)]
    (logger :info "Starting producer" {:cfg cfg :args args})
    (cond
      ; TODO: timeouts
      (some? io-producer) (async/go (io-producer args handle-result))
      (some? compute-producer) (async/take! (async/thread (compute-producer args)) handle-result)
      (some? projection) (async/take! (async/thread (projection args)) handle-result))))

(defn- register-periodicity [start-txn keypath update-pub {:keys [at-least-every-ms jitter-pct] :or {jitter-pct 0} :as periodicity}]
  (when (some? periodicity)
    (let [keypath-change-ch (async/chan)
          keypath-sentinel {:source ::periodicity
                            :keypath keypath}]
      (async/go-loop []
        ; idea: loop here waiting for our keypath to produce something OR for the timeout to elapse
        ; if the timeout elapses, start a transaction for our keypath
        (let [[update port] (async/alts! [keypath-change-ch
                                          (async/timeout (* at-least-every-ms (+ 1 (* (rand) jitter-pct))))])]
          (when (not= port keypath-change-ch)
            (start-txn ::periodicity keypath-sentinel))))
      keypath-sentinel)))

(defn- register-producer [logger
                          state-atom
                          descendants
                          all-ancestors
                          start-txn
                          register-post-txn-hook
                          send-update
                          pub
                          keypath
                          {:keys [param-keypaths
                                  transient-param-keypaths
                                  periodicity] :as cfg}]
  (let [update-ch (async/chan)
        transient-update-ch (async/chan)
        periodicity-ch (async/chan)
        param-ancestors (->> param-keypaths
                             (mapcat all-ancestors)
                             (into #{}))
        uniq-param-keypaths (->> param-keypaths
                                 (filter (comp not param-ancestors))
                                 (into #{}))]
    (async/go-loop []
      (when-let [upd (async/<! transient-update-ch)]
        (register-post-txn-hook #(handle-updates logger %1 keypath cfg [%2]))
        (recur)))
    (async/go-loop []
      (when-let [{:keys [txn-source] :as upd} (async/<! periodicity-ch)]
        (handle-updates logger send-update keypath cfg [(merge upd {:keypath keypath :origin-keypath keypath})])
        (recur)))
    (async/go-loop []
      (when-let [upd (async/<! update-ch)]
        ; based on the origin-keypath in upd, we find all of the params of ours that we expect to be fired.
        ; we wait for all of them before running
        (let [{updated-keypath :keypath :keys [txn-id origin-keypath]} upd
              orig-descendants (descendants origin-keypath)
              ; note: required-params is empty when (= updated-keypath keypath) by non-cyclicality
              required-params (clojure.set/intersection orig-descendants uniq-param-keypaths)
              needed-params (disj required-params updated-keypath)
              ; TODO: should definitely have a transaction timeout
              updates (loop [remaining needed-params
                             updates [upd]]
                        (if (empty? remaining)
                          updates
                          (let [{next-txn-id :txn-id
                                 next-keypath :keypath
                                 :as next-update} (async/<! update-ch)]
                            (if (or (not= next-txn-id txn-id)
                                    (not (contains? remaining next-keypath)))
                              (do (logger :error "Mixed transactions" {:txn-id txn-id :next-txn-id next-txn-id})
                                  ; TODO: don't continue
                                  [upd])
                              (recur (disj remaining next-keypath)
                                     (conj updates next-update))))))]
          (do (handle-updates logger send-update keypath cfg updates)
              (recur)))))
    (when-let [periodicity-sentinel (register-periodicity start-txn keypath pub periodicity)]
      (async/sub pub periodicity-sentinel periodicity-ch))
    (doseq [param-keypath uniq-param-keypaths]
      (async/sub pub param-keypath update-ch))
    (doseq [transient-param-keypath transient-param-keypaths]
      (async/sub pub transient-param-keypath transient-update-ch))
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
      - :transient-keypaths that list additional keypaths that should be selected from the state
        when collecting the partial-state to pass to the producer but which do not cause downstream
        producers to be invoked.
      - :param-keypath-validators that map keypaths from :param-keypaths to a validator function.
        If the validator function returns false, the producer will not be invoked.
      - :periodicity specifying :at-least-every-ms and an optional :jitter-pct.  The producer
        will be invoked at-least-every-ms, regardless of state changes.
     Cfg may specify:
      - a :logger that is a function of level (:info, :warn, :error), msg (string), and param map ({})."
  ([init producer-cfg-by-keypath & {:keys [logger] :or {logger prn}}]
   (init (fn [init-state] ; TODO: init should probably be a DAG we process
     ; TODO: handle (instance? Throwable init-state)
     (let [state (atom init-state)
           updates-chan (async/chan)
           start-txn-chan (async/chan)
           end-txn-chan (async/chan)
           post-txn-hook-chan (async/chan)
           post-txn-hooks (atom [])
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
                       (async/go (async/>! start-txn-chan {:action :start
                                                           :update-path []
                                                           :txn-id (java.util.UUID/randomUUID)
                                                           :origin-keypath keypath
                                                           :keypath keypath
                                                           :txn-source source})))
           make-update (fn [{:keys [update-path txn-id txn-source origin-keypath]} keypath new-state]
                          {:update-path (conj update-path keypath)
                           :txn-id txn-id
                           :txn-source txn-source
                           :origin-keypath origin-keypath
                           :keypath keypath
                           :new-state new-state})
           send-post-txn-hook-update (fn [upd keypath new-state]
                                      (async/put! post-txn-hook-chan (make-update upd keypath new-state)))
           register-post-txn-hook (fn [{:keys [txn-id]} f]
                                    ; TODO: check current txn-id
                                    (swap! post-txn-hooks (partial concat [f])))
           send-update (fn [upd keypath new-state]
                         (async/put!
                           (if (terminal-node? keypath) post-txn-hook-chan updates-chan)
                           (make-update upd keypath new-state)))]
       (async/go-loop []
         (when-let [{:keys [txn-id action] :as txn} (async/<! start-txn-chan)]
           (logger :info "Starting txn" {:txn txn})
           (if (not= action :start)
             (logger :error "Expected start transaction" {:txn txn})
             (do
               (async/>! updates-chan (make-update txn (:keypath txn) @state))
               (when-let [{:keys [new-state] end-txn-id :txn-id end-action :action :as end-txn} (async/<! end-txn-chan)]
                 (logger :info "Ending txn" {:txn end-txn})
                 (if (not (and (= end-txn-id txn-id)
                               (= end-action :end)))
                   (logger :error "Expected end transaction" {:start-txn txn :end-txn end-txn})
                   (send state #(merge % (apply dissoc new-state projections)))))))
           (recur)))
       (async/go-loop []
         (when-let [upd (async/<! post-txn-hook-chan)]
           (let [hooks @post-txn-hooks
                 hook (first hooks)]
             (if (empty? hooks)
               (async/>! end-txn-chan (assoc upd :action :end)) ; no more hooks, end the txn
               (do (swap! post-txn-hooks next)
                   (hook send-post-txn-hook-update upd)))
             (recur))))
       (doseq [[keypath cfg] producer-cfg-by-keypath]
         (register-producer logger state descendants all-ancestors start-txn register-post-txn-hook send-update p keypath cfg))
       state)))))

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
