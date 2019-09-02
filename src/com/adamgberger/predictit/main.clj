(ns com.adamgberger.predictit.main
  (:require [clojure.core.async :as async]
            [iapetos.core :as prometheus]
            [iapetos.collector.jvm :as jvm]
            [iapetos.standalone :as standalone]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.observable-state :as obs]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.venues.predictit :as pv]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as rcp-input]
            [com.adamgberger.predictit.inputs.rasmussen :as rasmussen-input]
            [com.adamgberger.predictit.inputs.yougov-weekly-registered :as yougov-weekly-input]
            [com.adamgberger.predictit.inputs.approval-rating-the-hill :as the-hill-input]
            [com.adamgberger.predictit.strategies.approval-rating-rcp :as strategy-rcp]
            [com.adamgberger.predictit.estimators.approval-rating-rcp :as rcp-estimator]
            [com.adamgberger.predictit.executors.default-executor :as exec])
  (:gen-class))

(defn pos-needing-orders [pos prev-orders]
  (->> pos
       (mapcat :contracts)
       (filter
        (fn [{:keys [market-id contract-id] {:keys [buy sell]} :orders}]
          (let [orders (get-in prev-orders [market-id contract-id :orders])
                buy-orders (filter #(#{:buy-yes :buy-no} (:trade-type %)) orders)
                sell-orders (filter #(#{:sell-yes :sell-no} (:trade-type %)) orders)
                qty-sum #(+ %1 (:qty %2))
                total-buys (reduce qty-sum 0 buy-orders)
                total-sells (reduce qty-sum 0 sell-orders)]
            (or (not= total-buys buy)
                (not= total-sells sell)))))
       (map (fn [{:keys [market-id]}]
              (->> pos
                   (filter #(= (:market-id %) market-id))
                   first)))
       (into #{})
       (into [])))

(defn deref-map [m]
  (->> m
       (map (fn [[k v]]
              [k
               (cond
                 (= (type v) clojure.lang.Agent) @v
                 (map? v) (deref-map v)
                 :else v)]))
       (into {})))

(defn state-watchdog [state end-chan]
  ; print out our full state every once in a while

  (l/log :info "Starting state watchdog")

  (let [handler #(l/log :info "Current state" {:state (deref-map state)})]
    (utils/repeatedly-until-closed
     handler
     60000
     #(l/log :warn "Stopping state watchdog")
     end-chan)))

(defn- async-transform-single
  "Applies 'xform', a function of one argument, to a single value in channel 'c' and
   invokes cb with the result"
  [xform cb c]
  (async/take!
    c
    #(-> % xform cb)))

(defn orig-investment [positions]
  (->> positions
       (mapcat :contracts)
       (map #(* (:qty %) (:avg-price-paid %)))
       (reduce + 0.0)))

(defn- strat-bankroll [bal pos allocation mkt-ids]
  (when (and (some? mkt-ids)
             (some? bal)
             (some? pos)
             (some? allocation))
    (let [relevant-pos (filter #(mkt-ids (:market-id %)) pos)
          strat-pos-investment (orig-investment relevant-pos)
          all-pos-investment (orig-investment pos)]
      (let [total-val (+ all-pos-investment bal)
            desired (* total-val allocation)]
        {:desired desired
         :current strat-pos-investment
         :cash (max 0.0 (- desired strat-pos-investment))}))))

(defn- suspend-until-pre-control
  ([suspend-until-key]
    (suspend-until-pre-control suspend-until-key (constantly true)))
  ([suspend-until-key suspend-pred]
    (fn pre-control [keypath cfg control-state args]
      (when-let [suspend-until (-> control-state suspend-until-key)]
        (if (.isBefore (java.time.Instant/now) suspend-until)
          (when (suspend-pred keypath cfg control-state args)
            {:decision :abort
             :reason {:anomaly :suspended}})
          {:control-state {suspend-until-key nil}})))))

(def ^:private venue-predictit-pre-control (suspend-until-pre-control :venue-predictit/suspend-until))

(defn- venue-predictit-post-control [keypath cfg control-state result]
  (let [now (java.time.Instant/now)]
    (when-let [{:keys [anomaly status headers]} (ex-data result)]
      (cond
        (= anomaly :trading-suspended) {:control-state {:venue-predictit/suspend-until (.plus now 30 java.time.temporal.ChronoUnit/MINUTES)}
                                        :decision :abort
                                        :reason {:anomaly :trading-suspended
                                                 :anomaly-at now}}
        (= status 429) {:control-state {:venue-predictit/suspend-until (.plus now (get headers "Retry-After" 60) java.time.temporal.ChronoUnit/SECONDS)}
                        :decision :abort
                        :reason {:anomaly :rate-limited
                                 :anomaly-at now}}))))

(defn- venue-predictit-post-control-array [keypath cfg control-state results]
  (let [result (->> results
                    (filter #(-> % ex-data some?))
                    first)]
    ; since we have many results, we never abort
    (dissoc (venue-predictit-post-control keypath cfg control-state result) :decision)))

(defn run-trading [cfg end-chan]
  (let [once-per-10-seconds 10000
        twice-per-minute 30000
        once-per-minute (* 2 twice-per-minute)
        once-per-10-minutes (* once-per-minute 10)
        metrics (-> (prometheus/collector-registry)
                    (jvm/initialize)
                    (prometheus/register
                      (prometheus/histogram :txn-duration-ms {:labels [:origin]})
                      (prometheus/counter :txn-count {:labels [:origin]})
                      (prometheus/histogram :producer-duration-ms {:labels [:key]})
                      (prometheus/counter :producer-count {:labels [:key]})
                      (prometheus/counter :abort-count {:labels [:key]})
                      (prometheus/counter :skip-count {:labels [:key :phase]})))
        obs-state (obs/observable-state
                    (fn make-initial-state [send-result]
                      (pv/make-venue
                        (:creds cfg)
                        (fn handle-venue [venue]
                          (send-result
                            (merge {:cfg (->> cfg
                                              (filter #(not= (first %) :creds))
                                              (into {}))}
                                   {:venue-predictit/venue venue})))))
                    {:venue-predictit/mkts
                      {:io-producer (fn predictit-mkts [{{:venue-predictit/keys [venue]} :partial-state} send-result]
                                      (v/available-markets venue send-result))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                     :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control}
                     :venue-predictit/bal
                      {:io-producer (fn predictit-bal [{{:venue-predictit/keys [venue]} :partial-state} send-result]
                                      (v/current-available-balance venue send-result))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control}
                     :venue-predictit/pos
                      {:io-producer (fn predictit-pos [{{:venue-predictit/keys [venue]} :partial-state} send-result]
                                      (v/positions venue send-result))
                       :periodicity {:at-least-every-ms once-per-minute
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control}
                     :venue-predictit/monitored-mkts
                      {:projection (fn predictit-monitored-mkts [{{:strategy-rcp/keys [mkts]} :partial-state}]
                                     mkts)
                       :param-keypaths [:strategy-rcp/mkts]}
                     :venue-predictit/mkts-by-id
                      {:projection (fn predictit-mkts-by-id [{{:venue-predictit/keys [monitored-mkts]} :partial-state}]
                                     (utils/index-by :market-id monitored-mkts))
                       :param-keypaths [:venue-predictit/monitored-mkts]}
                     :venue-predictit/contracts
                      {:io-producer (fn predictit-contracts [{{:venue-predictit/keys [venue monitored-mkts]} :partial-state} send-result]
                                      (->> monitored-mkts
                                           (map
                                            (fn contracts-for-mkt [{:keys [market-id market-url]}]
                                              (let [c (async/chan)]
                                                (v/contracts
                                                  venue
                                                  market-id
                                                  market-url
                                                  #(utils/async-put-once
                                                     c
                                                     (if (instance? Throwable %)
                                                       %
                                                       (mapv (fn add-contract-mkt-id [contract] (assoc contract :mkt-id market-id)) %))))
                                                c)))
                                           async/merge
                                           (async/into [])
                                           (async-transform-single
                                             #(reduce
                                                (fn reduce-contracts-by-mkt [by-mkt-id {:keys [mkt-id contract-id] :as contract}]
                                                  (if (instance? Throwable contract)
                                                    (reduced contract)
                                                    (assoc-in
                                                      by-mkt-id
                                                      [mkt-id contract-id]
                                                      contract)))
                                                {}
                                                (flatten %))
                                             send-result)))
                       :param-keypaths [:venue-predictit/venue :venue-predictit/monitored-mkts]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control}
                     :venue-predictit/order-books
                      {:io-producer (fn predictit-order-books [{{:venue-predictit/keys [venue contracts mkts-by-id]} :partial-state} send-result]
                                      (->> contracts
                                           (mapcat second)
                                           (map second)
                                           (map
                                            (fn order-book-for-contract [{:keys [mkt-id contract-id]}]
                                              (let [c (async/chan)
                                                    {:keys [market-url]} (get mkts-by-id mkt-id)]
                                                (v/order-book
                                                  venue
                                                  mkt-id
                                                  market-url
                                                  contract-id
                                                  #(utils/async-put-once
                                                     c
                                                     (if (instance? Throwable %)
                                                       %
                                                       {:mkt-id mkt-id
                                                        :contract-id contract-id
                                                        :order-book %})))
                                                c)))
                                           async/merge
                                           (async/into [])
                                           (async-transform-single
                                             #(reduce
                                                (fn reduce-order-books-by-mkt [by-mkt-by-contract {:keys [mkt-id contract-id order-book] :as maybe-err}]
                                                  (if (instance? Throwable maybe-err)
                                                    (reduced maybe-err)
                                                    (assoc-in
                                                      by-mkt-by-contract
                                                      [mkt-id contract-id]
                                                      order-book)))
                                                {}
                                                %)
                                             send-result)))
                       :periodicity {:at-least-every-ms once-per-minute
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue :venue-predictit/contracts :venue-predictit/mkts-by-id]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control}
                     :venue-predictit/orders
                      {:io-producer (fn predictit-orders [{:keys [prev]
                                                          {:venue-predictit/keys [venue pos mkts-by-id contracts]
                                                           rcp-mkts :strategy-rcp/mkts} :partial-state}
                                         send-result]
                                        (->> (pos-needing-orders pos prev)
                                             (concat (map (fn [mkt] (assoc mkt :contracts (->> (:market-id mkt) (get contracts) vals))) rcp-mkts))
                                             (group-by :market-id)
                                             vals
                                             (map first)
                                             (mapcat
                                              (fn [{:keys [market-id contracts]}]
                                                (map
                                                  (fn [{:keys [contract-id]}]
                                                    {:contract-id contract-id
                                                     :market-id market-id
                                                     :market-url (get-in mkts-by-id [market-id :market-url])})
                                                  contracts)))
                                             (map
                                              (fn orders-for-contract [{:keys [contract-id market-id market-url]}]
                                                (let [c (async/chan)]
                                                  (v/orders
                                                    venue
                                                    market-id
                                                    market-url
                                                    contract-id
                                                    #(do (async/put! c {:mkt-id market-id
                                                                        :contract-id contract-id
                                                                        :orders %})
                                                         (async/close! c)))
                                                  c)))
                                             async/merge
                                             (async/into [])
                                             (async-transform-single
                                              (fn [orders]
                                                (reduce
                                                  (fn [orders-by-mkt-by-contract {:keys [mkt-id contract-id orders]}]
                                                    (if (not-empty orders)
                                                      (assoc-in
                                                        orders-by-mkt-by-contract
                                                        [mkt-id contract-id]
                                                        {:valid? true
                                                         :orders orders})
                                                      orders-by-mkt-by-contract))
                                                  {}
                                                  orders))
                                              send-result)))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue
                                        :venue-predictit/pos
                                        :venue-predictit/mkts-by-id
                                        :venue-predictit/contracts
                                        :strategy-rcp/mkts]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control}
                     :venue-predictit/pos-by-contract-id
                      {:projection (fn predictit-pos-by-contract [{{:venue-predictit/keys [pos]} :partial-state}]
                                     (->> pos
                                          (mapcat :contracts)
                                          (utils/index-by :contract-id)))
                       :param-keypaths [:venue-predictit/pos]}
                     :venue-predictit/req-pos
                      {:projection (fn predictit-req-pos [{{:strategy-rcp/keys [trades]} :partial-state}]
                                     trades)
                       :param-keypaths [:strategy-rcp/trades]}
                     :inputs/rcp-current
                      {:io-producer (fn inputs-rcp-current [_ send-result]
                                        (rcp-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/rcp-hist
                      {:io-producer (fn inputs-rcp-hist [_ send-result]
                                        (rcp-input/get-hist send-result))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                     :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/rasmussen-current ; TODO: sometimes rasmussen doesn't return.  move to validator
                      {:io-producer (fn inputs-rasmussen-current [_ send-result]
                                        (rasmussen-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}
                       :param-keypaths []
                       :pre-control (suspend-until-pre-control
                                      :rasmussen/suspend-until
                                      (fn [keypath cfg control-state args]
                                        (let [next-ras (-> args :prev :next-expected)
                                              now (java.time.LocalDateTime/now utils/ny-time)
                                              window-mins 10
                                              diff-mins (-> now (.until next-ras java.time.temporal.ChronoUnit/MINUTES) Math/abs)]
                                          ; only allow suspension if diff-mins outside of window-mins
                                          (> diff-mins window-mins))))
                       :post-control (fn [keypath cfg control-state result]
                                        (let [now (java.time.Instant/now)]
                                          (when-let [{:keys [anomaly status headers]} (ex-data result)]
                                            (cond
                                              (= status 404) {:control-state {:rasmussen/suspend-until (.plus now 10 java.time.temporal.ChronoUnit/MINUTES)}
                                                              :decision :abort
                                                              :reason {:anomaly :not-found
                                                                       :anomaly-at now}}))))}
                     :inputs/yougov-weekly-registered-current
                      {:io-producer (fn inputs-yougov-weekly-registered-current [_ send-result]
                                        (yougov-weekly-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/the-hill-current
                      {:io-producer (fn inputs-the-hill-current [_ send-result]
                                        (the-hill-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-minute
                                    :jitter-pct 0.2}
                       :timeout-ms 20000
                       :param-keypaths []}
                     :estimators/approval-rcp
                      {:compute-producer (fn estimators-approval-rcp [{:keys [prev]
                                               {:keys [cfg]
                                                :inputs/keys [rcp-current
                                                              rcp-hist
                                                              rasmussen-current
                                                              yougov-weekly-registered-current
                                                              the-hill-current]} :partial-state}]
                                          (rcp-estimator/estimate
                                            (get-in cfg [:estimators :com.adamgberger.predictit.estimators.approval-rating-rcp/id :stats-for-days])
                                            rcp-current
                                            rcp-hist
                                            rasmussen-current
                                            yougov-weekly-registered-current
                                            the-hill-current))
                       :periodicity {:at-least-every-ms once-per-minute
                                     :jitter-pct 0.2}
                       :param-keypaths [:cfg
                                        :inputs/rcp-current
                                        :inputs/rcp-hist
                                        :inputs/rasmussen-current
                                        :inputs/yougov-weekly-registered-current
                                        :inputs/the-hill-current]}
                     :strategy-rcp/mkts
                      {:projection (fn strategy-rcp-mkts [{{:venue-predictit/keys [mkts]} :partial-state}]
                                    (->> mkts
                                         (filterv strategy-rcp/is-relevant-mkt)))
                       :param-keypaths [:venue-predictit/mkts]}
                     :strategy-rcp/tradable-mkts
                      {:projection (fn strateg-rcp-tradable-mkts
                                       [{{mkts :strategy-rcp/mkts
                                          contracts :venue-predictit/contracts} :partial-state}]
                                    (mapv
                                      (partial strategy-rcp/adapt-mkt contracts)
                                      mkts))
                       :param-keypaths [:strategy-rcp/mkts :venue-predictit/contracts]}
                     :strategy-rcp/prob-dist ; TODO: validate both some?
                      {:projection (fn strategy-rcp-prob-dist
                                       [{{est :estimators/approval-rcp
                                          tradable-mkts :strategy-rcp/tradable-mkts} :partial-state}]
                                    (strategy-rcp/calculate-prob-dists est tradable-mkts))
                       :param-keypaths [:estimators/approval-rcp :strategy-rcp/tradable-mkts]}
                     :strategy-rcp/latest-major-input-change
                      {:compute-producer (fn strategy-rcp-latest-major-input-change [{{est :estimators/approval-rcp} :partial-state}]
                                          (strategy-rcp/calculate-major-input-change est))
                       :param-keypaths [:estimators/approval-rcp]}
                     :strategy-rcp/req-pos
                      {:compute-producer (fn strategy-rcp-req-pos
                                             [{{:keys [cfg]
                                                :strategy-rcp/keys [tradable-mkts
                                                                    prob-dist
                                                                    latest-major-input-change]
                                                :venue-predictit/keys [order-books]
                                                :executor/keys [outstanding-orders]
                                                :estimators/keys [approval-rcp]} :partial-state
                                              :keys [prev]}]
                                          (strategy-rcp/calculate-trades
                                            (-> cfg :strats :com.adamgberger.predictit.strategies.approval-rating-rcp/id :hurdle-rate)
                                            tradable-mkts
                                            prob-dist
                                            latest-major-input-change
                                            approval-rcp
                                            order-books
                                            outstanding-orders
                                            prev))
                       :param-keypaths [:cfg
                                        :strategy-rcp/tradable-mkts
                                        :strategy-rcp/prob-dist
                                        :strategy-rcp/latest-major-input-change
                                        :venue-predictit/order-books
                                        :executor/outstanding-orders
                                        :estimators/approval-rcp]
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}}
                     :strategy-rcp/mkt-ids
                      {:projection (fn strategy-rcp-mkt-ids [{{:strategy-rcp/keys [mkts]} :partial-state}]
                                      (->> mkts (map :market-id) (into #{})))
                       :param-keypaths [:strategy-rcp/mkts]}
                     :strategy-rcp/bankroll
                      {:compute-producer (fn strategy-rcp-bankroll
                                             [{{:keys [cfg]
                                                :venue-predictit/keys [bal pos]
                                                :strategy-rcp/keys [mkt-ids]} :partial-state}]
                                          (let [strat-id :com.adamgberger.predictit.strategies.approval-rating-rcp/id
                                                allocation (get-in cfg [:allocations strat-id])]
                                            (strat-bankroll bal pos allocation mkt-ids)))
                       :param-keypaths [:cfg
                                        :venue-predictit/bal
                                        :venue-predictit/pos
                                        :strategy-rcp/mkt-ids]}
                     :strategy-rcp/desired-positions
                      {:compute-producer (fn strategy-rcp-desired-trades
                                             [{{:strategy-rcp/keys [bankroll req-pos]} :partial-state}]
                                          (exec/generate-desired-positions bankroll req-pos))
                       :param-keypaths [:strategy-rcp/bankroll
                                        :strategy-rcp/req-pos]
                       ; TODO: don't really want this periodic; just want immediately-executable-trades executable
                       :periodicity {:at-least-every-ms twice-per-minute
                                     :jitter-pct 0.2}}
                     :strategy-rcp/desired-trades
                      {:compute-producer (fn strategy-rcp-desired-trades
                                             [{{:venue-predictit/keys [order-books pos-by-contract-id]
                                                :executor/keys [outstanding-orders]
                                                :strategy-rcp/keys [desired-positions]} :partial-state}]
                                          (exec/generate-desired-trades desired-positions pos-by-contract-id outstanding-orders order-books))
                       :param-keypaths [:strategy-rcp/desired-positions
                                        :venue-predictit/pos-by-contract-id
                                        :executor/outstanding-orders
                                        :venue-predictit/order-books]}
                     :executor/outstanding-orders
                      {:compute-producer (fn executor-outstanding-orders
                                             [{{:venue-predictit/keys [orders]
                                                :executor/keys [executions]} :partial-state
                                                :keys [prev]}]
                                            (exec/update-orders prev orders executions))
                       :param-keypaths [:venue-predictit/orders]
                       :transient-param-keypaths [:executor/executions]}
                     :executor/desired-trades
                      {:projection (fn executor-desired-trades [{{:strategy-rcp/keys [desired-trades]} :partial-state}]
                                    desired-trades)
                       :param-keypaths [:strategy-rcp/desired-trades]}
                     :executor/immediately-executable-trades
                      {:compute-producer (fn executor-immediately-executable-trades
                                             [{{:executor/keys [desired-trades outstanding-orders]
                                                :venue-predictit/keys [bal mkts-by-id pos-by-contract-id contracts]} :partial-state}]
                                          (exec/generate-immediately-executable-trades desired-trades outstanding-orders bal mkts-by-id pos-by-contract-id contracts))
                       :param-keypaths [:executor/desired-trades
                                        :venue-predictit/bal
                                        :venue-predictit/pos-by-contract-id
                                        :venue-predictit/contracts
                                        :executor/outstanding-orders
                                        :venue-predictit/mkts-by-id]
                       ; TODO: can't make this periodic until desired-trades is actually a projection
                       ;:periodicity {:at-least-every-ms twice-per-minute
                       ;              :jitter-pct 0.2}}
                       }
                     :executor/executions
                      {:io-producer (fn exeuctor-executions
                                        [{{:executor/keys [immediately-executable-trades]
                                           :venue-predictit/keys [venue]} :partial-state}
                                         send-result]
                                      (exec/execute-orders venue immediately-executable-trades send-result))
                       :param-keypaths [:executor/immediately-executable-trades
                                        :venue-predictit/venue]
                       :pre-control venue-predictit-pre-control
                       :post-control venue-predictit-post-control-array}}
                    :logger l/log
                    :metrics metrics)]
    (standalone/metrics-server metrics {:port 8080})))

(defn -main
  [& args]
  (let [end-chan (async/chan)
        cfg (-> "config"
                slurp
                clojure.edn/read-string)]
    (run-trading cfg end-chan)))
