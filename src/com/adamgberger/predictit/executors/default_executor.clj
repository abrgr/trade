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

(def buy-types (set (vals buy-trade-type-for-side)))

(def sell-types (set (vals sell-trade-type-for-side)))

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

(defn- current-pos-to-sell [order-books-by-contract-id orders-by-contract-id mins {:keys [contract-id current side avg-price-paid]}]
  (let [trade-type (sell-trade-type-for-side side)
        {:keys [price]} (exec-utils/get-likely-fill
                         mins
                         avg-price-paid
                         (get order-books-by-contract-id contract-id)
                         (get orders-by-contract-id contract-id)
                         (java.math.MathContext. 2)
                         trade-type)]
    (if (nil? price)
      nil
      {:contract-id contract-id
       :target-price avg-price-paid
       :target-mins mins
       :trade-type trade-type
       :price price
       :qty 0})))

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
                          outstanding-orders-by-contract-id
                          (or (->> desired-pos first :target-mins) 60)))
                    (filter some?)))
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
                                 (let [likely-fill (exec-utils/get-likely-fill
                                                    (/ (:target-mins pos) 2)
                                                    (* (:avg-price-paid current) 1.1)
                                                    (-> venue-state
                                                        (get-in [venue-id :order-books mkt-id contract-id]))
                                                    orders
                                                    (java.math.MathContext. 2))]
                                   {:trade-type sell-current
                                    :qty current-qty
                                    :mkt-id mkt-id
                                    :contract-id contract-id
                                    :target-price (:est-value likely-fill)
                                    :price (:price likely-fill)}))
                order-cancel-buys (when-not (empty? bad-buys-to-cancel)
                                    (map
                                     (fn [ord]
                                       {:trade-type :cancel
                                        :target-price target-price
                                        :mkt-id mkt-id
                                        :contract-id contract-id
                                        :order-id (:order-id ord)})
                                     bad-buys-to-cancel))
                order-cancel-sells (when-not (empty? bad-sells-to-cancel)
                                     (map
                                      (fn [ord]
                                        {:trade-type :cancel
                                         :mkt-id mkt-id
                                         :contract-id contract-id
                                         :target-price target-price
                                         :order-id (:order-id ord)})
                                      bad-sells-to-cancel))
                order-place-buy (when (> remaining-qty 0)
                                  {:trade-type (:trade-type pos)
                                   :mkt-id mkt-id
                                   :contract-id contract-id
                                   :qty remaining-qty
                                   :target-price target-price
                                   :price desired-price})
                order-cancel-old-buys (map
                                       (fn [ord]
                                         {:trade-type :cancel
                                          :mkt-id mkt-id
                                          :contract-id contract-id
                                          :target-price target-price
                                          :order-id (:order-id ord)})
                                       old-buys-to-cancel)
                order-sell-old-pos (when (< desired-qty cur-qty)
                                     {:trade-type sell-current
                                      :mkt-id mkt-id
                                      :contract-id contract-id
                                      :qty (- cur-qty desired-qty)
                                      :target-price desired-price ; TODO: this ain't right but we don't really use this field
                                      :price desired-price})]
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

(defmulti submit-for-execution (fn [venue mkt-id {:keys [trade-type]}]
                                 (if (= :cancel trade-type)
                                   :cancel
                                   :trade)))
(defmethod submit-for-execution :cancel
  [venue mkt-id {:keys [order-id]}]
  (l/log :info "Cancelling order" {:mkt-id mkt-id :order-id order-id})
  (v/cancel-order venue mkt-id order-id)
  {:cancelled order-id})
(defmethod submit-for-execution :trade
  [venue mkt-id {:keys [contract-id trade-type qty price]}]
  (l/log :info "Submitting order" {:mkt-id mkt-id :contract-id contract-id :trade-type trade-type :qty qty :price price})
  {:submitted (v/submit-order venue mkt-id contract-id trade-type qty price)})

(defmulti trade-policy (fn [bp mkt contracts-by-id pos-by-contract-id orders-by-contract-id {:keys [trade-type]}]
                         (case trade-type
                           :cancel :cancel
                           :buy-yes :buy
                           :buy-no :buy
                           :sell-yes :sell
                           :sell-no :sell)))
(defmethod trade-policy :cancel
  [bp mkt contracts-by-id pos-by-contract-id orders-by-contract-id {:keys [contract-id]}]
  (let [{:keys [cancellable? qty price]} (get orders-by-contract-id contract-id)
        permitted? cancellable?
        bp-effect (if permitted? (* qty price) 0)]
    {:permitted? permitted?
     :buying-power (+ bp bp-effect)}))
(defmethod trade-policy :buy
  [bp mkt contracts-by-id pos-by-contract-id orders-by-contract-id {:keys [contract-id qty price]}]
  ; TODO: this doesn't account for negative-risk bets (e.g. buying all no's)
  (let [contract (get contracts-by-id contract-id)
        bp-effect (* -1 qty price)
        new-bp (+ bp bp-effect)
        permitted? (and (= (:status mkt) :open)
                        (:tradable? contract)
                        (> new-bp 0))]
    {:permitted? permitted?
     :buying-power (if permitted? new-bp bp)}))
(defmethod trade-policy :sell
  [bp mkt contracts-by-id pos-by-contract-id orders-by-contract-id {trade-qty :qty :keys [contract-id trade-type]}]
  (let [{:keys [tradable? qty side]} (get pos-by-contract-id contract-id)
        owned-side-ok? (= side (side-for-trade-type trade-type))
        owned-qty-ok? (>= qty trade-qty)
        permitted? (and (= (:status mkt) :open)
                        tradable?
                        owned-side-ok?
                        owned-qty-ok?)]
    {:permitted? permitted?
     :buying-power bp}))

(defn execute-trades [state end-chan venue-id req-pos-by-strat-id]
  (l/with-log :info "Executing trades"
    (let [venue-state @(:venue-state state)
          {:keys [cfg]} state
          venue (->> state
                     :venues
                     (filter #(= venue-id (v/id %)))
                     first)]
      (doseq [[strat-id req-positions-by-mkt-id] req-pos-by-strat-id]
        (let [outstanding-orders-by-contract-id-by-mkt-id (get-in venue-state [venue-id :orders])
              valid? (->> (mkt-ids-for-strat venue-state venue-id strat-id)
                          (select-keys outstanding-orders-by-contract-id-by-mkt-id)
                          (mapcat (comp vals second))
                          (every? :valid?))]
          (if-not valid?
            (l/log :warn "Not trading - invalid orders" {:strat-id strat-id
                                                         :req-positions-by-mkt-id req-positions-by-mkt-id
                                                         :outstanding-orders-by-contract-id-by-mkt-id outstanding-orders-by-contract-id-by-mkt-id})
            (doseq [[mkt-id req-positions] req-positions-by-mkt-id]
              (let [mkt (->> venue-state
                             venue-id
                             :mkts
                             (filter #(= mkt-id (:market-id %)))
                             first)
                    {:keys [bal]} (venue-id venue-state)
                    bankroll (strat-bankroll venue-state cfg strat-id venue-id)
                    outstanding-orders-by-contract-id (->> (get outstanding-orders-by-contract-id-by-mkt-id mkt-id)
                                                           (map (fn [[contract-id {:keys [orders]}]]
                                                                  [contract-id orders]))
                                                           (into {}))
                    total-order-dollars (reduce
                                         (fn [total [_ orders]]
                                           (reduce
                                            (fn [sum {:keys [price qty]}]
                                              (+ sum (* price qty)))
                                            total
                                            orders))
                                         0.0
                                         outstanding-orders-by-contract-id)]
                (if (nil? bankroll)
                  nil
                  (let [contracts-by-id (get-in venue-state [venue-id :contracts mkt-id])
                        desired-pos (map (partial desired-pos-for-req-pos bankroll) req-positions)
                        pos-by-contract-id (->> venue-state
                                                venue-id
                                                :pos
                                                (mapcat :contracts)
                                                (reduce
                                                 (fn [by-id c]
                                                   (assoc by-id (:contract-id c) c))
                                                 {}))
                        trades (adjust-desired-pos-for-actuals venue-state venue-id mkt-id desired-pos pos-by-contract-id outstanding-orders-by-contract-id)
                        trades-by-contract (reduce
                                            (fn [by-contract {:keys [contract-id] :as trade}]
                                              (update by-contract contract-id #(conj % trade)))
                                            {}
                                            trades)
                        trades-of-types (fn [is-type? trades]
                                          (filter #(is-type? (:trade-type %)) trades))
                        trades-to-execute-immediately (fn [[contract-id trades]]
                                                        (let [non-buy-types (comp not buy-types)
                                                              outstanding-non-buys (->> (get outstanding-orders-by-contract-id contract-id)
                                                                                        (trades-of-types non-buy-types))
                                                              non-buys (trades-of-types non-buy-types trades)
                                                              buys (trades-of-types buy-types trades)
                                                              allow-buys? (and (empty? non-buys)
                                                                               (empty? outstanding-non-buys))]
                                                          (if allow-buys?
                                                            buys
                                                            non-buys)))
                        trades-to-submit (mapcat trades-to-execute-immediately trades-by-contract)
                        trades-can-submit (reduce
                                           (fn [{:keys [trades bp] :as state} trade]
                                             (let [{:keys [permitted? buying-power]} (trade-policy bp mkt contracts-by-id pos-by-contract-id outstanding-orders-by-contract-id trade)]
                                               (if permitted?
                                                 {:trades (conj trades trade)
                                                  :bp buying-power}
                                                 (do (l/log :warn "Trade out of policy" {:trade trade
                                                                                         :bp bp
                                                                                         :trades trades})
                                                     state))))
                                           {:trades []
                                            :bp (- bal total-order-dollars)}
                                           trades-to-submit)
                        orders (map (partial submit-for-execution venue mkt-id) (:trades trades-can-submit))
                        to-remove (->> orders
                                       (map :cancelled)
                                       (filter some?)
                                       (group-by :contract-id)
                                       (map #(into #{} %)))
                        to-add (->> orders
                                    (map :submitted)
                                    (filter some?)
                                    (group-by :contract-id))]
                    (send
                     (:venue-state state)
                     (fn [s]
                       (update-in
                        s
                        [venue-id :orders mkt-id]
                        (fn [orders-by-contract-id]
                          (->> (keys orders-by-contract-id)
                               (concat (keys to-add))
                               (into #{})
                               (map
                                (fn [contract-id]
                                  (let [existing (get-in orders-by-contract-id [contract-id :orders])
                                        add (get to-add contract-id)
                                        rem (or (get to-remove contract-id) #{})]
                                    [contract-id
                                     {:valid? true
                                      :orders (->> add
                                                   (concat (filter #(->> % :order-id rem not) existing))
                                                   (into []))}])))
                               (into {}))))))
                    (l/log :info "Trades" {:desired-pos desired-pos
                                           :pos-by-contract-id pos-by-contract-id
                                           :outstanding-orders-by-contract-id outstanding-orders-by-contract-id
                                           :trades trades
                                           :orders orders})))))))))))
