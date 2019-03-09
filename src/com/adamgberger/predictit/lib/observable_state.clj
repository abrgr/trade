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

(defn- on-result [is-terminal update-path send-update new-state prev keypath {:keys [validator] :as cfg} result]
    (let [valid? (or (nil? validator)
                     (validator result))
          effective-result (if valid?
                               result
                               (with-meta prev {::invalid-value result}))]
        (send-update is-terminal update-path keypath (assoc new-state keypath effective-result))))

(defn- handle-updates [terminal-nodes
                       send-update
                       keypath
                       {:keys [param-keypaths io-producer compute-producer] :as cfg}
                       updates]
    (let [update-keypaths (map :keypath updates)
          new-states (map :new-state updates)
          new-state (apply merge new-states) ; TODO: confirm that we can never get 2 updates to the same key
          partial-state (select-keys new-state param-keypaths)
          prev (keypath new-state)
          args {:partial-state partial-state
                :prev prev
                :key keypath}
          on-result (partial on-result (terminal-nodes keypath) update send-update new-state prev keypath cfg)]
        (cond
            ; TODO: timeouts
            (some? io-producer) (async/go (io-producer args on-result))
            (some? compute-producer) (async/go (let [res (async/thread (compute-producer args))]
                                                    (on-result (async/<! res)))))))

(defn- register-producer [logger
                          state-atom
                          descendants
                          all-ancestors
                          terminal-nodes
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
                                            (do (logger "Mixed transactions" {:txn-id txn-id :next-txn-id next-txn-id})
                                                (reduced {:remaining #{}
                                                          :updates []}))
                                            {:remaining (disj remaining next-keypath)
                                             :updates (conj updates next-update)})))
                                {:remaining needed-params
                                 :updates [update]}
                                needed-params)]
                    (when-not (empty? remaining)
                        (logger "Remaining updates not empty" {:remaining remaining
                                                               :updates updates
                                                               :needed-params needed-params
                                                               :required-params required-params}))
                    (do (handle-updates terminal-nodes send-update keypath cfg updates)
                        (recur)))))
        (when (some? periodicity)
            ; TODO: handle periodicity with periodicity of params.  e.g. if param invoked every 1 second and we're invoked every 2, just skip
            (async/sub pub ::periodically update-ch))
        (doseq [param-keypath uniq-param-keypaths]
            (async/sub pub param-keypath update-ch))))

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
    ([init producer-cfg-by-keypath & cfg]
        (let [{:keys [logger]} (into {} cfg) ; TODO: how do you default a destructure like this?
              state (atom init)
              updates-chan (async/chan)
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
              terminal-nodes (->> producer-cfg-by-keypath
                                  keys
                                  (filter #(= (count (next-nodes %)) 0))
                                  (into #{}))
              send-update (fn [is-terminal {:keys [update-path txn-id origin-keypath]} keypath new-state]
                            (async/go (async/>! updates-chan {:update-path (conj update-path keypath)
                                                              :txn-id txn-id
                                                              :origin-keypath origin-keypath
                                                              :keypath keypath
                                                              :new-state new-state})
                                      (when is-terminal (send state #(merge % new-state)))))]
            (doseq [[keypath cfg] producer-cfg-by-keypath]
                (register-producer (or logger prn) state descendants all-ancestors terminal-nodes send-update p keypath cfg))
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

(s/def ::at-least-every-ms int?)

(s/def ::jitter-pct double?)

(s/def ::periodicity
    (s/keys :req-un [::at-least-every-ms]
            :opt-un [::jitter-pct]))

(s/def ::keypath (s/coll-of keyword?))

(s/def ::param-keypaths (s/coll-of ::keypath))

(s/def ::param-keypath-validators (s/map-of ::keypath ::pred))

(s/def ::producer-cfg
    (s/keys :req-un [(or ::io-producer ::compute-producer)]
            :opt-un [::validator ::param-keypath-validators ::periodicity ::param-keypaths]))

(s/def ::observable-state any?)

(s/fdef observable-state
    :args (s/cat :init map?
                 :producer-cfg (s/map-of ::keypath ::producer-cfg))
    :ret ::observable-state)