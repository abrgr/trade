(ns com.adamgberger.predictit.executors.default-executor
  (:require [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.execution-utils :as exec-utils]
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

(defn- no-matching-pos [poss {:keys [contract-id]}]
  (->> poss
       (filter #(= (:contract-id %) contract-id))
       empty?))

(defn- current-pos-to-sell [order-books-by-contract-id mins {:keys [contract-id current side avg-price-paid]}]
  (let [trade-type (sell-trade-type-for-side side)]
    {:contract-id contract-id
    :target-price avg-price-paid
    :target-mins mins
    :trade-type trade-type
    :price (:price
              (exec-utils/get-likely-fill
                mins
                avg-price-paid
                (get order-books-by-contract-id contract-id)
                (java.math.MathContext. 2)
                trade-type))
    :qty 0}))

(defn- adjust-desired-pos-for-actuals [venue-state venue-id mkt-id desired-pos current-pos-by-contract-id outstanding-orders-by-contract-id]
  (->> desired-pos
       ; generate sells for any position that we no longer have a desired-pos for
       (concat (->> current-pos-by-contract-id
                    vals
                    (filter #(= mkt-id (:market-id %)))
                    (filter (partial no-matching-pos desired-pos))
                    (map (partial
                          current-pos-to-sell
                          (get-in venue-state [venue-id :order-books mkt-id])
                          (or (->> desired-pos first :target-mins) 60)))))
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
                  current-holding (* current-qty (:avg-price-paid current))
                  opp-buy (-> desired-side opposite-side buy-trade-type-for-side)
                  desired-side-sell (-> desired-side sell-trade-type-for-side)
                  {desired-price :price desired-qty :qty} pos
                  close-enough-order? #(< (Math/abs (double (- (:price %) desired-price))) 0.05)
                  ords-by-should-cancel (->> pos
                                             :trade-type
                                             ords-by-trade-type
                                             (partition-by close-enough-order?)
                                             (map (fn [items] [(->> items first close-enough-order? not)
                                                               items]))
                                             (into {}))
                  old-buys-to-cancel (get ords-by-should-cancel true)
                  still-active-old-buys (get ords-by-should-cancel false)
                  ord-qty (->> still-active-old-buys
                               (map :qty)
                               (reduce + 0))
                  cur-qty (if (= desired-side current-side)
                              current-qty
                              0)
                  ord-amt (->> still-active-old-buys
                               (reduce #(+ %1 (* (:price %2) (:qty %2))) 0.0))
                  cur-amt (+ current-holding ord-amt)
                  max-qty (Math/floor (/ (- 850.0 cur-amt) desired-price))
                  remaining-qty (min max-qty (- desired-qty cur-qty ord-qty))
                  bad-buys-to-cancel (opp-buy ords-by-trade-type)
                  bad-sells-to-cancel (desired-side-sell ords-by-trade-type)
                  order-sell-cur (when (and (> current-qty 0)
                                            (not= desired-side current-side))
                                    {:trade-type sell-current
                                     :qty current-qty
                                     :contract-id contract-id
                                     :target-price target-price
                                     :price (exec-utils/get-likely-fill
                                               (/ (:fill-mins pos) 2)
                                               (* (:avg-price-paid current) 1.1)
                                               (-> venue-state
                                                   (get-in [venue-id :order-books mkt-id contract-id]))
                                               (java.math.MathContext. 2))})
                  order-cancel-buys (when-not (empty? bad-buys-to-cancel)
                                        (map
                                            (fn [ord]
                                              {:trade-type :cancel
                                               :target-price target-price
                                               :contract-id contract-id
                                               :order-id (:order-id ord)})
                                            bad-buys-to-cancel))
                  order-cancel-sells (when-not (empty? bad-sells-to-cancel)
                                        (map
                                            (fn [ord]
                                              {:trade-type :cancel
                                              :contract-id contract-id
                                              :target-price target-price
                                              :order-id (:order-id ord)})
                                            bad-sells-to-cancel))
                  order-place-buy (when (> remaining-qty 0)
                                    {:trade-type (:trade-type pos)
                                     :contract-id contract-id
                                     :qty remaining-qty
                                     :target-price target-price
                                     :price (:price pos)})
                  order-cancel-old-buys (map
                                          (fn [ord]
                                            {:trade-type :cancel
                                            :contract-id contract-id
                                            :target-price target-price
                                            :order-id (:order-id ord)})
                                          old-buys-to-cancel)
                  order-sell-old-pos (when (< desired-qty cur-qty)
                                        {:trade-type sell-current
                                         :contract-id contract-id
                                         :qty (- cur-qty desired-qty)
                                         :target-price (:price pos) ; TODO: this ain't right but we don't really use this field
                                         :price (:price pos)})]
              (->>
                [; we hold something we don't want; sell it
                 order-sell-cur
                 ; we have buy orders out for the wrong side; cancel them
                 order-cancel-buys
                 ; we have sell orders out for the wrong side; cancel them
                 order-cancel-sells
                 ; our buy order
                 order-place-buy
                 ; canceling our old orders with bad prices
                 order-cancel-old-buys
                 ; sell old positions we no longer want
                 order-sell-old-pos]
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
                      trades (adjust-desired-pos-for-actuals @(:venue-state state) venue-id mkt-id desired-pos pos-by-contract-id outstanding-orders-by-contract-id)]
                  ; TODO: keep track of orders we expect
                  ; TODO: execute these trades
                  (l/log :info "Trades" {:desired-pos desired-pos
                                        :pos-by-contract-id pos-by-contract-id
                                        :outstanding-orders-by-contract-id outstanding-orders-by-contract-id
                                        :trades trades}))))))))))