(ns com.adamgberger.predictit.lib.observable-state
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [iapetos.core :as prometheus])
  (:gen-class))

(defn- validate
  [producer-cfg-map state]
  (every?
   (fn [[keypath {:keys [validator]}]]
     (or (nil? validator)
         (validator (get-in state keypath))))
   state))

(defn- with-merge-meta [obj m]
  (if (instance? clojure.lang.IObj obj)
    (vary-meta obj #(merge % m))
    obj))

(defn- invoke-control [control keypath cfg control-state args]
  (let [control-info (when (some? control)
                           (control keypath cfg @control-state args))]
    (some->> control-info :control-state (swap! control-state merge))
    control-info))

(defn- default-post-control [keypath cfg _ result]
  (when (instance? Throwable result)
    {:decision :abort
     :reason {:anomaly :exception
              :exception result
              :ex-data (ex-data result)}}))

(defn- on-result [metrics control-state start-time upd logger update-path send-update abort-txn state prev keypath {:keys [post-control] :as cfg} state-update result]
  (logger :info "Finished producer" {:keypath keypath :result result :txn-id (get upd :txn-id)})
  (let [new-val (with-merge-meta result {::updated-at (java.time.Instant/now)
                                         ::prev (if (instance? clojure.lang.IObj prev)
                                                    (vary-meta prev #(dissoc % ::prev))
                                                    nil)})
        state-update' (merge state-update {keypath new-val})
        upd' (assoc upd :update-path update-path)
        metric-labels {:key keypath}
        control-info (invoke-control (or post-control default-post-control) keypath cfg control-state result)]
    (prometheus/observe
      (metrics :producer-duration-ms metric-labels)
      (-> start-time
          (java.time.Duration/between (java.time.Instant/now))
          .toMillis))
    (prometheus/inc (metrics :producer-count metric-labels))
    (case (:decision control-info)
      :abort (abort-txn upd keypath (:reason control-info))
      :skip (do (logger :warn "Skipping value" {:keypath keypath :reason (:reason control-info) :result result})
                (prometheus/inc (metrics :skip-count {:key keypath :phase :post}))
                (send-update upd' state-update))
      (send-update upd' keypath state-update'))))

(defmacro throw->ret [& body]
  `(try
     ~@body
     (catch Exception e#
       e#)))

(defn- safe-put! [ch v]
  (if (nil? v)
    (async/close! ch)
    (async/put! ch v)))

(defn- handle-updates [logger
                       metrics
                       control-state
                       send-update
                       abort-txn
                       keypath
                       {:keys [param-keypaths
                               transient-param-keypaths
                               io-producer
                               compute-producer
                               projection
                               timeout-ms
                               pre-control] :as cfg}
                       updates]
  (let [state-updates (map :state-update updates)
        state (apply merge (-> updates first :orig-state) state-updates)
        partial-state (select-keys state (concat param-keypaths transient-param-keypaths))
        prev (keypath state)
        args {:partial-state partial-state
              :prev prev
              :key keypath}
        upd (last updates)
        update-path (if (= (count updates) 1)
                      (-> updates first :update-path)
                      (mapv :update-path updates)) ; TODO: this seems wrong
        handle-result (partial on-result metrics control-state (java.time.Instant/now) upd logger update-path send-update abort-txn state prev keypath cfg (apply merge nil (map :state-update updates)))
        result-ch (async/chan)
        control-info (invoke-control pre-control keypath cfg control-state args)]
    (case (:decision control-info)
      :abort (abort-txn upd keypath (:reason control-info))
      :skip (do (logger :warn "Skipping invocation" {:reason (:reason control-info) :keypath keypath :args args})
                (prometheus/inc (metrics :skip-count {:key keypath :phase :pre}))
                (handle-result prev))
      (do (logger :info "Starting producer" {:keypath keypath :args args :txn-id (-> updates first (get :txn-id)) :update-path update-path})

          (async/go
            (let [timeout-ch (async/timeout (or timeout-ms 60000))
                  [v p] (async/alts! [result-ch timeout-ch])]
              (if (= p timeout-ch)
                (do (logger :warn "Producer timeout" {:cfg cfg :keypath keypath :args args :updates updates})
                    (handle-result (ex-info "Timeout" {:anomaly :timeout :keypath keypath})))
                (handle-result v))))

          (cond
            (some? io-producer) (async/go
                                  (try
                                    (io-producer args #(safe-put! result-ch %))
                                    (catch Exception e
                                      (safe-put! result-ch e))))
            (some? compute-producer) (async/take!
                                       (async/thread (throw->ret (compute-producer args)))
                                       #(safe-put! result-ch %))
            (some? projection) (async/take!
                                 (async/thread (throw->ret (projection args)))
                                 #(safe-put! result-ch %)))))))

(defn- register-periodicity [start-txn
                             keypath
                             update-pub
                             {:keys [at-least-every-ms jitter-pct] :or {jitter-pct 0} :as periodicity}
                             all-ancestors
                             producer-cfg-by-keypath]
  (when (some? periodicity)
    (let [keypath-change-ch (async/chan)
          keypath-sentinel {:source ::periodicity
                            :keypath keypath}
          any-periodicity-ancestors (some #(-> % producer-cfg-by-keypath :periodicity some?) all-ancestors)
          timeout-ms #(* at-least-every-ms (+ 1 (* (rand) jitter-pct)))
          first-ms (if any-periodicity-ancestors (timeout-ms) (* 100 (+ 1 (* (rand) jitter-pct))))]
      (async/go-loop [ms first-ms]
        ; idea: loop here waiting for our keypath to produce something OR for the timeout to elapse
        ; if the timeout elapses, start a transaction for our keypath
        (let [[update port] (async/alts! [keypath-change-ch
                                          (async/timeout ms)])]
          (when (not= port keypath-change-ch)
            (start-txn ::periodicity keypath-sentinel))
          (recur (timeout-ms))))
      keypath-sentinel)))

(defn- post-txn-abort [logger upd keypath reason]
  (logger :warn "Cannot abort txn in post-txn hook" {:upd upd :keypath keypath :reason reason}))

(defn- register-producer [logger
                          metrics
                          descendants
                          all-ancestors
                          start-txn
                          register-post-txn-hook
                          send-update
                          abort-txn
                          pub
                          keypath
                          control-state
                          {:keys [param-keypaths
                                  transient-param-keypaths
                                  periodicity] :as cfg}
                          producer-cfg-by-keypath]
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
        (register-post-txn-hook upd #(handle-updates logger metrics control-state %1 (partial post-txn-abort logger) keypath cfg [%2]))
        (recur)))
    (async/go-loop []
      (when-let [{:keys [txn-source] :as upd} (async/<! periodicity-ch)]
        (handle-updates logger metrics control-state send-update abort-txn keypath cfg [(merge upd {:keypath keypath :origin-keypath keypath :update-path []})])
        (recur)))
    (async/go-loop []
      (when-let [upd (async/<! update-ch)]
        ; based on the origin-keypath in upd, we find all of the params of ours that we expect to be fired.
        ; we wait for all of them before running
        ; TODO: need to ensure that any projections we depend on also run before us
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
                              (do (logger :error "Mixed transactions" {:txn-id txn-id
                                                                       :next-txn-id next-txn-id
                                                                       :remaining remaining
                                                                       :keypath keypath
                                                                       :updates updates
                                                                       :next-keypath next-keypath
                                                                       :needed-params needed-params})
                                  ; TODO: don't continue
                                  [upd])
                              (recur (disj remaining next-keypath)
                                     (conj updates next-update))))))]
          (handle-updates logger metrics control-state send-update abort-txn keypath cfg updates)
          (recur))))
    (when-let [periodicity-sentinel (register-periodicity start-txn keypath pub periodicity all-ancestors producer-cfg-by-keypath)]
      (async/sub pub periodicity-sentinel periodicity-ch))
    (doseq [param-keypath uniq-param-keypaths]
      (async/sub pub param-keypath update-ch))
    (doseq [transient-param-keypath transient-param-keypaths]
      (async/sub pub transient-param-keypath transient-update-ch))))

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
      - a :logger that is a function of level (:info, :warn, :error), msg (string), and param map ({}).
      - a :metrics that behaves like an iapetos (https://github.com/xsc/iapetos) registry."
  ([init producer-cfg-by-keypath & {:keys [logger metrics] :or {logger prn}}]
   (init (fn [init-state] ; TODO: init should probably be a DAG we process
     ; TODO: handle (instance? Throwable init-state)
     (let [state (atom init-state)
           control-state (atom {})
           updates-chan (async/chan)
           start-txn-chan (async/chan)
           end-txn-chan (async/chan)
           post-txn-hook-chan (async/chan)
           post-txn-hook-changes-chan (async/chan)
           post-txn-hook-changes-pub (async/pub post-txn-hook-changes-chan (constantly :all))
           post-txn-hooks (atom [])
           p (async/pub updates-chan :keypath)
           all-ancestors (ancestors-for-all (->> producer-cfg-by-keypath
                                                 (map (fn [[keypath {:keys [param-keypaths]}]]
                                                        {:task keypath
                                                         :depends-on param-keypaths}))
                                                 (into {})))
           make-nexts (fn [k]
                        (->> producer-cfg-by-keypath
                             (reduce
                              (fn [descendants [keypath cfg]]
                                (->> cfg
                                     k
                                     (reduce
                                      (fn [descs param]
                                        (update descs param #(conj %1 keypath)))
                                      descendants)))
                              {})))
           next-nodes (make-nexts :param-keypaths)
           transient-next-nodes (make-nexts :transient-param-keypaths)
           descendants (transitive-closure next-nodes)
           terminal-node? (->> producer-cfg-by-keypath
                               keys
                               (filter #(empty? (next-nodes %)))
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
           make-update (fn [{:keys [update-path txn-id txn-source origin-keypath orig-state]} keypath state-update]
                          {:update-path (conj update-path keypath)
                           :txn-id txn-id
                           :txn-source txn-source
                           :origin-keypath origin-keypath
                           :keypath keypath
                           :orig-state orig-state
                           :state-update state-update})
           send-post-txn-hook-update (fn [upd keypath state-update]
                                      (async/put! post-txn-hook-chan (make-update upd keypath state-update)))
           register-post-txn-hook (fn [{:keys [txn-id]} f]
                                    ; TODO: check current txn-id
                                    (swap! post-txn-hooks conj f))
           abort-txn (fn [upd keypath reason]
                        (logger :warn "Aborting txn" {:txn-id (:txn-id upd :reason reason)})
                        (prometheus/inc (metrics :abort-count {:key keypath}))
                        (swap! post-txn-hooks [])
                        (async/>! end-txn-chan (merge upd {:action :end :state-update {}})))
           send-update (fn [upd keypath state-update]
                         (let [new-upd (make-update upd keypath state-update)
                               transients-to-wait-for (transient-next-nodes keypath)]
                           (cond
                             ; if we are a terminal node AND we have transients that react to us, we wait for them to register their post-txn-hooks
                             (and (terminal-node? keypath)
                                  (not-empty transients-to-wait-for)) (let [c (async/chan)]
                                                                        (async/sub post-txn-hook-changes-pub :all c)
                                                                        (async/go
                                                                          (async/>! updates-chan new-upd)
                                                                          (doseq [_ transients-to-wait-for]
                                                                            (async/<! c))
                                                                          (async/unsub post-txn-hook-changes-pub :all c)
                                                                          (async/>! post-txn-hook-chan new-upd)))
                             (terminal-node? keypath) (async/put! post-txn-hook-chan new-upd)
                             :else (async/put! updates-chan new-upd))))]
       (add-watch post-txn-hooks :post-txn-hooks-changes (fn [k _ _ _] (async/put! post-txn-hook-changes-chan k)))
       (async/go-loop []
         (when-let [{:keys [txn-id action origin-keypath] :as txn} (async/<! start-txn-chan)]
           (let [start-time (java.time.Instant/now)]
             (logger :info "Starting txn" {:txn txn})
             (if (not= action :start)
               (logger :error "Expected start transaction" {:txn txn})
               (do
                 (async/>! updates-chan (make-update (assoc txn :orig-state @state) (:keypath txn) nil))
                 (when-let [{:keys [state-update] end-txn-id :txn-id end-action :action :as end-txn} (async/<! end-txn-chan)]
                   (logger :info "Ending txn" {:txn-id (-> end-txn :txn :txn-id)})
                   (prometheus/observe (metrics :txn-duration-ms {:origin origin-keypath}) (-> start-time (java.time.Duration/between (java.time.Instant/now)) .toMillis))
                   (prometheus/inc (metrics :txn-count {:origin origin-keypath}))
                   (if (not (and (= end-txn-id txn-id)
                                 (= end-action :end)))
                     (logger :error "Expected end transaction" {:start-txn txn :end-txn end-txn})
                     ; TODO: until we ensure that projections are run before dependent producers, need to store in state
                     ;(swap! state #(merge % (apply dissoc state-update projections))))))))
                     (swap! state #(merge % state-update)))))))
           (recur)))
       (async/go-loop []
         (when-let [upd (async/<! post-txn-hook-chan)]
           (let [hooks @post-txn-hooks
                 hook (first hooks)]
             (if (empty? hooks)
               (async/>! end-txn-chan (assoc upd :action :end)) ; no more hooks, end the txn
               (do (swap! post-txn-hooks (comp vec next))
                   (hook send-post-txn-hook-update upd))) ; TODO: weird that this assumes a call to send-post-txn-hook-update
             (recur))))
       (doseq [[keypath cfg] producer-cfg-by-keypath]
         (register-producer logger metrics descendants all-ancestors start-txn register-post-txn-hook send-update abort-txn p keypath control-state cfg producer-cfg-by-keypath))
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
