(ns com.adamgberger.predictit.executors.default-executor
  (:require [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.venues.venue :as v]))

(defn mkt-ids-for-strat [venue-state venue-id strat-id]
  (->> venue-state
       venue-id
       :req-order-books
       strat-id
       (map :market-id)
       (into #{})))

(defn orig-investment [positions]
  (->> positions
       (mapcat :contracts)
       (map #(* (:qty %) (:avg-price-paid %)))
       (reduce + 0.0)))

(defn- get-orders [state venue-id venue-state mkt-id]
  (let [venue (->> state
                   :venues
                   (filter #(= (v/id %) venue-id))
                   first)
        contract-ids (-> venue-state
                         venue-id
                         :contracts
                         (get mkt-id)
                         keys)
        mkt-name (->> venue-state
                      venue-id
                      :mkts
                      (filter #(= (:market-id %) mkt-id))
                      first
                      :market-name)
        orders-by-contract-id (->> contract-ids
                                   (map
                                     (fn [contract-id]
                                       [contract-id (v/orders venue mkt-id mkt-name contract-id)]))
                                   (into {}))]
    (send (:venue-state state) #(assoc-in % [venue-id :orders mkt-id] orders-by-contract-id))
    orders-by-contract-id))

(defn- outstanding-orders-for-strat [state venue-id venue-state strat-id]
  (let [{:keys [outstanding-orders]} venue-state
        mkt-ids (mkt-ids-for-strat venue-state venue-id strat-id)
        orders-by-mkt-id (map
                          (fn [mkt-id] [mkt-id (get-in venue-state [:orders mkt-id])])
                          mkt-ids)]
    (->> orders-by-mkt-id
         (map
          (fn [[mkt-id orders]]
            [mkt-id
             (if (nil? orders)
                (get-orders state venue-id venue-state mkt-id)
                orders)]))
         (into {}))))

(defn- desired-pos-for-req-pos [bankroll req-pos]
  {:contract-id (:contract-id req-pos)
   :target-price (:est-value req-pos)
   :target-mins (:fill-mins req-pos)
   :trade-type (:trade-type req-pos)
   :price (:price req-pos)
   :qty (Math/floor (/ (* (:desired bankroll) (:weight req-pos)) (:price req-pos)))})

(def side-for-trade-type
  {:buy-yes :yes
   :sell-yes :yes
   :buy-no :no
   :sell-no :no})

(def opposite-side
  {:yes :no
   :no :yes})

(def buy-trade-type-for-side
  {:yes :buy-yes
   :no :buy-no})

(def sell-trade-type-for-side
  {:yes :sell-yes
   :no :sell-no})

(defn- orders-by-trade-type [orders]
  (reduce
    (fn [by-type order]
      (update by-type (:trade-type order) #(conj % order)))
    {:buy-yes []
     :sell-yes []
     :buy-no []
     :sell-no []}
    orders))

(defn- adjust-desired-pos-for-actuals [desired-pos current-pos-by-contract-id outstanding-orders-by-contract-id]
  (->> desired-pos
       (mapcat
          (fn [pos]
            (let [contract-id (:contract-id pos)
                  desired-side (->> pos :trade-type side-for-trade-type)
                  current (or (get current-pos-by-contract-id contract-id)
                              {:side desired-side
                               :qty 0
                               :avg-price-paid 0.0})
                  orders (or (get outstanding-orders-by-contract-id contract-id) [])
                  current-side (:side current)
                  current-qty (:qty current)
                  target-price (:target-price pos)
                  sell-current (sell-trade-type-for-side current-side)
                  ords-by-trade-type (orders-by-trade-type orders)
                  current-holding (* current-qty (:avg-price-paid current))]
              (->>
                [; we hold something we don't want; sell it
                 (when (and (> current-qty 0)
                             (not= desired-side current-side))
                     {:trade-type sell-current
                      :qty current-qty
                      :contract-id contract-id
                      :target-price target-price
                      :price :?}) ; TODO
                 ; we have buy orders out for the wrong side; cancel them
                 (let [opp-buy (-> desired-side opposite-side buy-trade-type-for-side)
                       ords-to-cancel (opp-buy ords-by-trade-type)]
                     (when-not (empty? ords-to-cancel)
                       (map
                          (fn [ord]
                            {:trade-type :cancel
                             :target-price target-price
                             :contract-id contract-id
                             :order-id (:order-id ord)})
                          ords-to-cancel)))
                 ; we have sell orders out for the wrong side; cancel them
                 (let [desired-side-sell (-> desired-side sell-trade-type-for-side)
                       ords-to-cancel (desired-side-sell ords-by-trade-type)]
                     (when-not (empty? ords-to-cancel)
                       (map
                          (fn [ord]
                            {:trade-type :cancel
                             :contract-id contract-id
                             :target-price target-price
                             :order-id (:order-id ord)})
                          ords-to-cancel)))
                 ; we want to buy
                 (let [desired-price (:price pos)
                       still-valid-order? #(< (Math/abs (double (- (:price %) desired-price))) 0.05)
                       cur-ords (-> pos
                                    :trade-type
                                    ords-by-trade-type)
                       ords (->> cur-ords
                                 (filter still-valid-order?))
                       ords-to-cancel (->> cur-ords
                                           (filter (comp not still-valid-order?)))
                       ord-qty (->> ords
                                    (map :qty)
                                    (reduce + 0))
                       cur-qty (if (= desired-side current-side)
                                   current-qty
                                   0)
                       ord-amt (->> ords
                                    (reduce #(+ %1 (* (:price %2) (:qty %2))) 0.0))
                       cur-amt (+ current-holding ord-amt)
                       max-qty (/ (- 850.0 cur-amt) desired-price)
                       remaining-qty (min max-qty (- (:qty pos) cur-qty ord-qty))]
                    (concat
                      ; our buy order
                      [(when (> remaining-qty 0)
                        {:trade-type (:trade-type pos)
                         :contract-id contract-id
                         :qty remaining-qty
                         :target-price target-price
                         :price (:price pos)})]
                      ; canceling our old orders with bad prices
                      (map
                        (fn [ord]
                          {:trade-type :cancel
                           :contract-id contract-id
                           :target-price target-price
                           :order-id (:order-id ord)})
                        ords-to-cancel)))]
                flatten
                (filter some?)
                (into [])))))))

(defn strat-bankroll [venue-state cfg strat-id venue-id]
  (let [{:keys [bal pos]} (venue-id venue-state)
        allocation (get-in cfg [:allocations strat-id])
        mkt-ids (mkt-ids-for-strat venue-state venue-id strat-id)
        relevant-pos (filter #(mkt-ids (:market-id %)) pos) ; TODO: making a silly assumption that strat:market is 1:1
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

(defn execute-trades [state end-chan venue-id req-pos-by-strat-id]
  (l/with-log :info "Executing trades"
    (let [venue-state @(:venue-state state)
          {:keys [cfg]} state]
      (doseq [[strat-id req-positions-by-mkt-id] req-pos-by-strat-id]
        (let [outstanding-orders-by-contract-id-by-mkt-id (outstanding-orders-for-strat state venue-id venue-state strat-id)]
          (doseq [[mkt-id req-positions] req-positions-by-mkt-id]
            (let [bankroll (strat-bankroll venue-state cfg strat-id venue-id)
                  outstanding-orders-by-contract-id (get outstanding-orders-by-contract-id-by-mkt-id mkt-id)]
              (if (nil? bankroll)
                nil
                (let [desired-pos (map (partial desired-pos-for-req-pos bankroll) req-positions)
                      pos-by-contract-id (->> venue-state
                                              venue-id
                                              :pos
                                              (mapcat :contracts)
                                              (reduce
                                                (fn [by-id c]
                                                  (assoc by-id (:contract-id c) c))
                                                {}))
                      trades (adjust-desired-pos-for-actuals desired-pos pos-by-contract-id outstanding-orders-by-contract-id)]
                  ; TODO: keep track of orders we expect
                  ; TODO: execute these trades
                  (l/log :info "Trades" {:desired-pos desired-pos
                                        :pos-by-contract-id pos-by-contract-id
                                        :outstanding-orders-by-contract-id outstanding-orders-by-contract-id
                                        :trades trades}))))))))))