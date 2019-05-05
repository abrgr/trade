(ns com.adamgberger.predictit.main
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.observable-state :as obs]
            [com.adamgberger.predictit.venues :as vs]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.strategies :as strats]
            [com.adamgberger.predictit.inputs :as inputs]
            [com.adamgberger.predictit.estimators :as estimators]
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

(defn start-strategies [cfg state end-chan]
  (reduce
   (fn [strats next]
     (merge strats (next cfg state end-chan)))
   {}
   (strats/all)))

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

(defn async-transform-single
  "Applies 'xform', a function of one argument, to a single value in channel 'c' and
   invokes cb with the result"
  [xform cb c]
  (async/take!
    c
    #(-> % xform cb)))

(defn run-trading [cfg end-chan]
  (let [state {:venues (->> (vs/venue-makers)
                            (map #(% (:creds cfg)))
                            (into []))
               :cfg (->> cfg
                         (filter #(not= (first %) :creds))
                         (into {}))
               :venue-state (l/logging-agent "venue-state" (agent {}))
               :strats {}
               :inputs (l/logging-agent "inputs" (agent {}))
               :estimates (l/logging-agent "estimates" (agent {}))}
        state (assoc-in state [:strats] (start-strategies (:strats cfg) state end-chan))
        twice-per-minute 30000
        once-per-minute (* 2 twice-per-minute)
        once-per-10-minutes (* once-per-minute 10)
        obs-state (obs/observable-state
                    (fn [send-result]
                      (v/make-venue
                        cfg
                        #(send-result
                          (merge {:cfg cfg} {:venue-predictit/venue %}))))
                    {:venue-predictit/executions
                      {:compute-producer execute-trades
                       :periodicity {:at-least-every-ms twice-per-minute
                                     :jitter-pct 0.2}
                       :param-keypaths [:cfg
                                        :venue-predictit/mkts
                                        :venue-predictit/contracts
                                        :venue-predictit/venue
                                        :venue-predictit/monitored-market-ids
                                        :venue-predictit/req-pos
                                        :venue-predictit/pos
                                        :venue-predictit/bal
                                        :venue-predictit/orders
                                        :venue-predictit/order-books]}
                     :venue-predictit/mkts
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
                      {:projection (fn [{{:strategy-rcp/keys [monitored-mkts]} :partial-state}]
                                     monitored-mkts)
                       :param-keypaths [:strategy-rcp/monitored-mkts]}
                     :venue-predictit/mkts-by-id
                      {:projection (fn [{{:strategy-rcp/keys [mkts]} :partial-state}]
                                     (group-by mkts :market-id))
                       :param-keypaths [:strategy-rcp/mkts]}
                     :venue-predictit/contracts
                      {:io-producer (fn [{{:venue-predictit/keys [venue monitored-mkts]} :partial-state} send-result]
                                      (->> monitored-mkts
                                           (map
                                            (fn [{:keys [market-id market-url]}]
                                              (let [c (async/chan)]
                                                (v/contracts
                                                  venue market-id
                                                  market-url
                                                  #(do (async/put! c (assoc % :mkt-id market-id))
                                                       (async/close! c))
                                                c))))
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
                                                %))))
                       :param-keypaths [:venue-predictit/venue venue-predictit/monitored-mkts]}
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
                                                %))))
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
                      }

                    :logger l/log)]
    (estimators/start-all (:estimators cfg) state end-chan)
    (inputs/start-all state end-chan)
    (state-watchdog state end-chan)
    (async/<!! end-chan)))

(defn -main
  [& args]
  (let [end-chan (async/chan)
        cfg (-> "config"
                slurp
                clojure.edn/read-string)]
    (run-trading cfg end-chan)))
