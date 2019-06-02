(ns com.adamgberger.predictit.main
  (:require [clojure.core.async :as async]
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
  (let [relevant-pos (filter #(mkt-ids (:market-id %)) pos)
        strat-pos-investment (orig-investment relevant-pos)
        all-pos-investment (orig-investment pos)]
    (if (and (some? bal)
             (some? pos)
             (some? allocation))
      (let [total-val (+ all-pos-investment bal)
            desired (* total-val allocation)]
        {:desired desired
         :current strat-pos-investment
         :cash (max 0.0 (- desired strat-pos-investment))}))))

(defn run-trading [cfg end-chan]
  (let [once-per-10-seconds 10000
        twice-per-minute 30000
        once-per-minute (* 2 twice-per-minute)
        once-per-10-minutes (* once-per-minute 10)
        obs-state (obs/observable-state
                    (fn [send-result]
                      (pv/make-venue
                        (:creds cfg)
                        (fn [venue]
                          (send-result
                            (merge {:cfg (->> cfg
                                              (filter #(not= (first %) :creds))
                                              (into {}))}
                                   {:venue-predictit/venue venue})))))
                    {:venue-predictit/mkts
                      {:io-producer (fn [{{:venue-predictit/keys [venue]} :partial-state} send-result]
                                      (v/available-markets venue send-result))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                     :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue]}
                     :venue-predictit/bal
                      {:io-producer (fn [{{:venue-predictit/keys [venue]} :partial-state} send-result]
                                      (v/current-available-balance venue send-result))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue]}
                     :venue-predictit/pos
                      {:io-producer (fn [{{:venue-predictit/keys [venue]} :partial-state} send-result]
                                      (v/positions venue send-result))
                       :periodicity {:at-least-every-ms once-per-minute
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue]}
                     :venue-predictit/monitored-mkts
                      {:projection (fn [{{:strategy-rcp/keys [mkts]} :partial-state}]
                                     mkts)
                       :param-keypaths [:strategy-rcp/mkts]}
                     :venue-predictit/mkts-by-id
                      {:projection (fn [{{:venue-predictit/keys [monitored-mkts]} :partial-state}]
                                     (utils/index-by :market-id monitored-mkts))
                       :param-keypaths [:venue-predictit/monitored-mkts]}
                     :venue-predictit/contracts
                      {:io-producer (fn [{{:venue-predictit/keys [venue monitored-mkts]} :partial-state} send-result]
                                      (->> monitored-mkts
                                           (map
                                            (fn [{:keys [market-id market-url]}]
                                              (let [c (async/chan)]
                                                (v/contracts
                                                  venue
                                                  market-id
                                                  market-url
                                                  #(do (async/put! c (assoc % :mkt-id market-id))
                                                       (async/close! c)))
                                                c)))
                                           async/merge
                                           (async/into [])
                                           (async-transform-single
                                             #(reduce
                                                (fn [by-mkt-id {:keys [mkt-id contract-id] :as contract}]
                                                  (assoc-in
                                                    by-mkt-id
                                                    [mkt-id contract-id]
                                                    contract))
                                                {}
                                                %)
                                             send-result)))
                       :param-keypaths [:venue-predictit/venue :venue-predictit/monitored-mkts]}
                     :venue-predictit/order-books
                      {:io-producer (fn [{{:venue-predictit/keys [venue contracts mkts-by-id]} :partial-state} send-result]
                                      (->> contracts
                                           (mapcat second)
                                           (map second)
                                           (map
                                            (fn [{:keys [mkt-id contract-id]}]
                                              (let [c (async/chan)
                                                    {:keys [market-url]} (get mkts-by-id mkt-id)]
                                                (v/order-book
                                                  venue
                                                  mkt-id
                                                  market-url
                                                  contract-id
                                                  #(do (async/put! c {:mkt-id mkt-id
                                                                      :contract-id contract-id
                                                                      :order-book %})
                                                       (async/close! c))))))
                                           async/merge
                                           (async/into [])
                                           (async-transform-single
                                             #(reduce
                                                (fn [by-mkt-by-contract {:keys [mkt-id contract-id order-book]}]
                                                  (assoc-in
                                                    by-mkt-by-contract
                                                    [mkt-id contract-id]
                                                    order-book))
                                                {}
                                                %)
                                             send-result)))
                       :periodicity {:at-least-every-ms once-per-minute
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue :venue-predictit/contracts :venue-predictit/mkts-by-id]}
                     :venue-predictit/orders
                      {:io-producer (fn [{:keys [prev]
                                          {:venue-predictit/keys [venue pos mkts-by-id]} :partial-state}
                                         send-result]
                                        (->> (pos-needing-orders pos prev)
                                             (mapcat
                                              (fn [{:keys [market-id contracts]}]
                                                (map
                                                  (fn [{:keys [contract-id]}]
                                                    {:contract-id contract-id
                                                     :market-id market-id
                                                     :market-url (get-in mkts-by-id [market-id :market-url])}))))
                                             (map
                                              (fn [{:keys [contract-id market-id market-url]}]
                                                (let [c (async/chan)]
                                                  (v/orders
                                                    venue
                                                    market-id
                                                    market-url
                                                    contract-id
                                                    #(do (async/put! c {:mkt-id market-id
                                                                        :contract-id contract-id
                                                                        :orders %})
                                                         (async/close! c))))))
                                             async/merge
                                             (async/into [])
                                             (async-transform-single
                                              (fn [orders]
                                                (reduce
                                                  (fn [orders-by-mkt-by-contract {:keys [mkt-id contract-id orders]}]
                                                    (assoc-in
                                                      orders-by-mkt-by-contract
                                                      [mkt-id contract-id]
                                                      {:valid? true
                                                      :orders orders}))
                                                  {}
                                                  orders))
                                              send-result)))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                    :jitter-pct 0.2}
                       :param-keypaths [:venue-predictit/venue :venue-predictit/pos :venue-predictit/mkts-by-id]}
                     :venue-predictit/pos-by-contract-id
                      {:projection (fn [{{:venue-predictit/keys [pos]} :partial-state}]
                                     (->> pos
                                          (mapcat :contracts)
                                          (utils/index-by :contract-id)))
                       :param-keypaths [:venue-predictit/pos]}
                     :venue-predictit/req-pos
                      {:projection (fn [{{:strategy-rcp/keys [trades]} :partial-state}]
                                     trades)
                       :param-keypaths [:strategy-rcp/trades]}
                     :inputs/rcp-current
                      {:io-producer (fn [_ send-result]
                                        (rcp-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/rcp-hist
                      {:io-producer (fn [_ send-result]
                                        (rcp-input/get-hist send-result))
                       :periodicity {:at-least-every-ms once-per-10-minutes
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/rasmussen-current ; TODO: sometimes rasmussen doesn't return.  move to validator
                      {:io-producer (fn [_ send-result]
                                        (rasmussen-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/yougov-weekly-registered-current
                      {:io-producer (fn [_ send-result]
                                        (yougov-weekly-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-10-seconds
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :inputs/the-hill-current
                      {:io-producer (fn [_ send-result]
                                        (the-hill-input/get-current send-result))
                       :periodicity {:at-least-every-ms once-per-minute
                                    :jitter-pct 0.2}
                       :param-keypaths []}
                     :estimators/approval-rcp
                      {:compute-producer (fn [{:keys [prev]
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
                      {:projection (fn [{{:venue-predictit/keys [mkts]} :partial-state}]
                                    (->> mkts
                                         (filterv strategy-rcp/is-relevant-mkt)))
                       :param-keypaths [:venue-predictit/mkts]}
                     :strategy-rcp/tradable-mkts
                      {:projection (fn [{{mkts :strategy-rcp/mkts
                                          contracts :venue-predictit/contracts} :partial-state}]
                                    (mapv
                                      (partial strategy-rcp/adapt-mkt contracts)
                                      mkts))
                       :param-keypaths [:strategy-rcp/mkts :venue-predictit/contracts]}
                     :strategy-rcp/prob-dist ; TODO: validate both some?
                      {:projection (fn [{{est :estimators/approval-rcp
                                          tradable-mkts :strategy-rcp/tradable-mkts} :partial-state}]
                                    (strategy-rcp/calculate-prob-dists est tradable-mkts))
                       :param-keypaths [:estimators/approval-rcp :strategy-rcp/tradable-mkts]}
                     :strategy-rcp/latest-major-input-change
                      {:compute-producer (fn [{{est :estimators/approval-rcp} :partial-state}]
                                          (strategy-rcp/calculate-major-input-change est))
                       :param-keypaths [:estimators/approval-rcp]}
                     :strategy-rcp/req-pos
                      {:compute-producer (fn [{{:keys [cfg]
                                                :strategy-rcp/keys [tradable-mkts
                                                                    prob-dist
                                                                    latest-major-input-change]
                                                :venue-predictit/keys [order-books]
                                                :executor/keys [outstanding-orders]
                                                :estimators/keys [approval-rcp]} :partial-state}]
                                          (strategy-rcp/calculate-trades
                                            (-> cfg :com.adamgberger.strategies.approval-rating-rcp/id :hurdle-rate)
                                            tradable-mkts
                                            prob-dist
                                            latest-major-input-change
                                            approval-rcp
                                            order-books
                                            outstanding-orders))
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
                      {:projection (fn [{{:strategy-rcp/keys [mkts]} :partial-state}]
                                      (->> mkts (map :market-id) (into #{})))}
                     :strategy-rcp/bankroll
                      {:compute-producer (fn [{{:keys [cfg]
                                                :venue-predictit/keys [bal pos]
                                                :strategy-rcp/keys [mkt-ids]} :partial-state}]
                                          (let [strat-id :com.adamgberger.predictit.strategies.approval-rating-rcp/id
                                                allocation (get-in cfg [:allocations strat-id])]
                                            (strat-bankroll bal pos allocation mkt-ids)))
                       :param-keypaths [:cfg
                                        :venue-predictit/bal
                                        :venue-predictit/pos
                                        :strategy-rcp/mkt-ids]}
                     :strategy-rcp/desired-trades
                      {:compute-producer (fn [{{:venue-predictit/keys [mkts-by-id pos bal order-books pos-by-contract-id]
                                                :executor/keys [outstanding-orders]
                                                :strategy-rcp/keys [bankroll req-pos]} :partial-state}]
                                          (exec/generate-desired-trades bankroll mkts-by-id req-pos pos-by-contract-id bal outstanding-orders order-books))
                       :param-keypaths [:strategy-rcp/bankroll
                                        :venue-predictit/mkts-by-id
                                        :venue-predictit/pos-by-contract-id
                                        :strategy-rcp/req-pos
                                        :venue-predictit/pos
                                        :venue-predictit/bal
                                        :executor/outstanding-orders
                                        :venue-predictit/order-books]}
                     :executor/outstanding-orders
                      {:compute-producer (fn [{{:venue-predictit/keys [orders]
                                                :executor/keys [executions]} :partial-state
                                               :keys [prev]}]
                                            (exec/update-orders prev orders executions))
                       :param-keypaths [:venue-predictit/orders]
                       :transient-param-keypaths [:executor/executions]}
                     :executor/desired-trades
                      {:projection (fn [{{:strategy-rcp/keys [desired-trades]} :partial-state}]
                                    desired-trades)
                       :param-keypaths [:strategy-rcp/desired-trades]}
                     :executor/immediately-executable-trades
                      {:compute-producer (fn [{{:strategy-rcp/keys [desired-trades]
                                                :executor/keys [outstanding-orders]
                                                :venue-predictit/keys [bal mkts-by-id pos-by-contract-id contracts]} :partial-state}]
                                          (exec/generate-immediately-executable-trades desired-trades outstanding-orders bal mkts-by-id pos-by-contract-id contracts))
                       :param-keypaths [:executor/desired-trades
                                        :venue-predictit/bal
                                        :venue-predictit/pos-by-contract-id
                                        :venue-predictit/contracts
                                        :executor/outstanding-orders
                                        :venue-predictit/mkts-by-id]
                       :periodicity {:at-least-every-ms twice-per-minute
                                     :jitter-pct 0.2}}
                     :executor/executions
                      {:io-producer (fn [{{:executor/keys [immediately-executable-trades]
                                           :venue-predictit/keys [venue]} :partial-state}
                                         send-result]
                                      (exec/execute-orders venue immediately-executable-trades send-result))
                      :param-keypaths [:executor/immediately-executable-trades
                                       :venue-predictit/venue]}}
                    :logger l/log)]))

(defn -main
  [& args]
  (let [end-chan (async/chan)
        cfg (-> "config"
                slurp
                clojure.edn/read-string)]
    (run-trading cfg end-chan)))
