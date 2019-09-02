(ns com.adamgberger.predictit.executors.default-executor
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.execution-utils :as exec-utils]
            [com.adamgberger.predictit.venues.venue :as v]))

(defn mkt-ids-for-strat [req-order-books strat-id]
  (->> req-order-books
       strat-id
       (map :market-id)
       (into #{})))

(defn- round-down-to-multiple-of [mult n]
  (int (* (quot n mult) mult)))

(defn- desired-pos-for-req-pos [bankroll req-pos]
  {:contract-id (:contract-id req-pos)
   :target-price (:est-value req-pos)
   :target-mins (:fill-mins req-pos)
   :trade-type (:trade-type req-pos)
   :price (:price req-pos)
   :qty (round-down-to-multiple-of 5 (/ (* (:desired bankroll) (:weight req-pos)) (:price req-pos)))})

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

(def action-for-trade-type
  {:buy-yes :buy
   :sell-yes :sell
   :buy-no :buy
   :sell-no :sell})

(def opp-action-for-trade-type
  {:buy-yes :sell-yes
   :sell-yes :buy-yes
   :buy-no :sell-no
   :sell-no :buy-no})

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

(defmulti should-keep-order? (fn [info desired-pos order] {:order-action (-> :trade-type order action-for-trade-type)
                                                           :same-side (= (-> :trade-type order side-for-trade-type)
                                                                         (-> :trade-type desired-pos side-for-trade-type))}))
(defmethod should-keep-order? {:order-action :buy :same-side true}
  [{:keys [cur-qty total-buy-qty]} {desired-qty :qty desired-price :price} {:keys [price]}]
  (and (>= desired-qty (+ cur-qty total-buy-qty)) ; TODO: this doesn't account for wanting an extra 20 but having 2 orders in for 15; both would be canceled
       (< (Math/abs (double (- price desired-price))) 0.03)))
(defmethod should-keep-order? {:order-action :buy :same-side false}
  [_ _ _]
  false)
(defmethod should-keep-order? {:order-action :sell :same-side true}
  [{:keys [cur-qty]} {desired-qty :qty desired-price :price} {:keys [price]}]
  (and (< desired-qty cur-qty)
       (< (Math/abs (double (- price desired-price))) 0.03)))
(defmethod should-keep-order? {:order-action :sell :same-side false}
  [_ {desired-price :price} {:keys [price]}]
  (< (Math/abs (double (- price desired-price))) 0.03))

(defn- adjust-desired-pos-for-actuals [order-books mkt-id desired-pos current-pos-by-contract-id outstanding-orders-by-contract-id]
  (->> desired-pos
       ; generate sells for any position that we no longer have a desired-pos for
       (concat (->> current-pos-by-contract-id
                    vals
                    (filter #(= mkt-id (:market-id %)))
                    (filter (partial no-matching-pos desired-pos))
                    (map (partial
                          current-pos-to-sell
                          (get order-books mkt-id)
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
                current-holding (* current-qty (:avg-price-paid current))
                desired-side-sell (-> desired-side sell-trade-type-for-side)
                opp-side-sell (-> desired-side opposite-side sell-trade-type-for-side)
                {desired-price :price desired-qty :qty} pos
                cur-qty (if (= desired-side current-side)
                          current-qty
                          0)
                opp-qty (if (not= desired-side current-side)
                          current-qty
                          0)
                total-buy-qty (->> orders
                                   (filter #(= (:trade-type %) (:trade-type pos)))
                                   (map :qty)
                                   (reduce + 0))
                ords-by-should-keep (->> orders
                                         (group-by (partial should-keep-order? {:cur-qty cur-qty :total-buy-qty total-buy-qty} pos)))
                old-ords-to-cancel (get ords-by-should-keep false)
                still-active-old-ords (get ords-by-should-keep true)
                our-side-buy-ords (->> still-active-old-ords
                                       (filter #(= (:trade-type %) (:trade-type pos))))
                our-side-sell-qty (->> still-active-old-ords
                                       (filter #(= (:trade-type %) desired-side-sell))
                                       (map :qty)
                                       (reduce + 0))
                opp-side-sell-qty (->> still-active-old-ords
                                       (filter #(= (:trade-type %) opp-side-sell))
                                       (map :qty)
                                       (reduce + 0))
                remaining-buy-qty (->> our-side-buy-ords
                                       (map :qty)
                                       (reduce + 0))
                ord-amt (->> our-side-buy-ords
                             (reduce #(+ %1 (* (:price %2) (:qty %2))) 0.0))
                cur-amt (+ current-holding ord-amt)
                max-qty (Math/floor (/ (- 850.0 cur-amt) desired-price))
                remaining-qty (min max-qty (- desired-qty cur-qty remaining-buy-qty))
                order-sell-opp (when (and (not= desired-side current-side)
                                          (> current-qty 0)
                                          (< opp-side-sell-qty current-qty))
                                 (let [likely-fill (exec-utils/get-likely-fill
                                                    2 ; if we're on the wrong side, just get out
                                                    (- 1 target-price)
                                                    (get-in order-books [mkt-id contract-id])
                                                    orders
                                                    (java.math.MathContext. 2))]
                                   {:trade-type sell-current
                                    :qty (- current-qty opp-side-sell-qty)
                                    :mkt-id mkt-id
                                    :contract-id contract-id
                                    :target-price (:est-value likely-fill)
                                    :price (:price likely-fill)
                                    :reason :wrong-side}))
                order-place-buy (when (> remaining-qty 0)
                                  {:trade-type (:trade-type pos)
                                   :mkt-id mkt-id
                                   :contract-id contract-id
                                   :qty remaining-qty
                                   :target-price target-price
                                   :price desired-price
                                   :reason :buy-more})
                order-cancel-old-ords (map
                                       (fn [ord]
                                         {:trade-type :cancel
                                          :mkt-id mkt-id
                                          :contract-id contract-id
                                          :target-price target-price
                                          :order-id (:order-id ord)
                                          :reason :outdated-order})
                                       old-ords-to-cancel)
                order-sell-old-pos (when (and (>= desired-qty 0)
                                              (< desired-qty (- cur-qty our-side-sell-qty)))
                                     {:trade-type sell-current
                                      :mkt-id mkt-id
                                      :contract-id contract-id
                                      :qty (- cur-qty desired-qty our-side-sell-qty)
                                      :target-price desired-price ; TODO: this ain't right but we don't really use this field
                                      :price desired-price
                                      :reason :sell-some})]
            (->>
             [; we hold something we don't want; sell it
              order-sell-opp
              ; our buy order
              order-place-buy
              ; canceling our old orders with bad prices, on the wrong side, or no longer needed
              order-cancel-old-ords
              ; sell old positions we no longer want
              order-sell-old-pos]
             flatten
             (filter some?)
             (into [])))))
       (into [])))

(defmulti submit-for-execution (fn [venue {:keys [mkt-id trade-type]} send-result]
                                 (if (= :cancel trade-type)
                                   :cancel
                                   :trade)))
(defmethod submit-for-execution :cancel
  [venue {:keys [mkt-id contract-id order-id price qty]} send-result]
  (l/log :info "Cancelling order" {:mkt-id mkt-id :order-id order-id :contract-id contract-id})
  (v/cancel-order
    venue
    mkt-id
    order-id
    (utils/cb-wrap-error
      send-result
      (fn [_]
        (send-result {:cancelled {:mkt-id mkt-id
                      :contract-id contract-id
                      :order-id order-id
                      :trade-type :cancel
                      :qty qty
                      :price price}})))))
(defmethod submit-for-execution :trade
  [venue {:keys [mkt-id contract-id trade-type qty price]} send-result]
  (l/log :info "Submitting order" {:mkt-id mkt-id :contract-id contract-id :trade-type trade-type :qty qty :price price})
  (v/submit-order
    venue
    mkt-id
    contract-id
    trade-type
    qty
    price
    (utils/cb-wrap-error
      send-result
      #(send-result {:submitted %}))))

(defmulti trade-policy (fn [bp mkt contracts-by-id pos-by-contract-id orders-by-contract-id {:keys [trade-type]}]
                         (case trade-type
                           :cancel :cancel
                           :buy-yes :buy
                           :buy-no :buy
                           :sell-yes :sell
                           :sell-no :sell)))
(defmethod trade-policy :cancel
  [bp mkt contracts-by-id pos-by-contract-id orders-by-contract-id {:keys [contract-id order-id]}]
  (let [{:keys [cancellable? qty price]} (->> (get orders-by-contract-id contract-id)
                                              (filter #(= (:order-id %) order-id))
                                              first)
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

(defn- get-orders-by-contract-id [orders mkt-id]
  (->> (get orders mkt-id)
       (map (fn [[contract-id {:keys [orders]}]]
             [contract-id orders]))
       (into {})))

(defn generate-desired-positions [bankroll
                                  req-pos]
  (->> req-pos
    (map
      (fn [[mkt-id req-positions]]
        (if (nil? bankroll)
          nil
          (let [desired-pos (mapv (partial desired-pos-for-req-pos bankroll) req-positions)]
            [mkt-id desired-pos]))))
    (filter some?)
    (into {})))

(defn generate-desired-trades [desired-pos
                               pos-by-contract-id
                               orders
                               order-books]
  (->> desired-pos
    (map
      (fn [[mkt-id desired-positions]]
        (let [outstanding-orders-by-contract-id (get-orders-by-contract-id orders mkt-id)]
          [mkt-id (adjust-desired-pos-for-actuals order-books mkt-id desired-positions pos-by-contract-id outstanding-orders-by-contract-id)])))
    (filter some?)
    (into {})))

(defn generate-immediately-executable-trades [trades-by-mkt-id orders bal mkts-by-id pos-by-contract-id contracts]
  (->> trades-by-mkt-id
       (mapcat
        (fn [[mkt-id trades]]
          (let [trades-by-contract (reduce
                                     (fn [by-contract {:keys [contract-id] :as trade}]
                                       (update by-contract contract-id #(conj % trade)))
                                     {}
                                     trades)
                total-order-dollars (reduce
                                     (fn [total [_ orders]]
                                       (reduce
                                        (fn [sum {:keys [price qty]}]
                                          (+ sum (* price qty)))
                                        total
                                        orders))
                                     0.0
                                     (get-orders-by-contract-id orders mkt-id))
                trades-of-types (fn [is-type? trades]
                                  (filter #(is-type? (:trade-type %)) trades))
                trades-to-execute-immediately (fn [[contract-id trades]]
                                                (let [buys (trades-of-types buy-types trades)
                                                      sells (trades-of-types sell-types trades)
                                                      cancels (trades-of-types #{:cancel} trades)]
                                                  (cond
                                                    (not-empty cancels) cancels
                                                    (not-empty sells) sells
                                                    (not-empty buys) buys)))
                trades-to-submit (mapcat trades-to-execute-immediately trades-by-contract)]
            (->> trades-to-submit
                 (reduce
                  (fn [{:keys [trades bp] :as state} trade]
                    (let [outstanding-orders-by-contract-id (get-orders-by-contract-id orders mkt-id)
                          mkt (get mkts-by-id mkt-id)
                          contracts-by-id (get contracts mkt-id)
                          {:keys [permitted? buying-power]} (trade-policy bp mkt contracts-by-id pos-by-contract-id outstanding-orders-by-contract-id trade)]
                      (if permitted?
                        {:trades (conj trades trade)
                         :bp buying-power}
                        (do (l/log :warn "Trade out of policy" {:trade trade
                                                                :bp bp
                                                                :trades trades})
                            state))))
                  {:trades []
                   :bp (- bal total-order-dollars)})
                 :trades))))
       (into [])))

(defn execute-orders [venue orders-to-submit send-result]
  (->> (async/to-chan orders-to-submit)
       (utils/async-map
         (fn [order out-ch]
           (submit-for-execution venue order #(utils/async-put-once out-ch (if (instance? Exception %) {:err %} %)))))
       (async/into [])
       (#(async/take! % send-result))))

(defn update-orders [prev-orders orders submitted-orders]
  ; TODO: we shouldn't know we're running in observable-state
  (let [orders-changed (-> orders meta :com.adamgberger.predictit.lib.observable-state/txn-start-val (not= orders))
        existing-order? (->> prev-orders
                             vals
                             (mapcat vals)
                             (mapcat :orders)
                             (map :order-id)
                             (into #{}))
        to-remove (->> submitted-orders
                       (map :cancelled)
                       (filter some?)
                       (group-by :contract-id)
                       (map #(vector (first %) (->> % second (map :order-id) (into #{}))))
                       (into {}))
        to-add (->> submitted-orders
                    (map :submitted)
                    (filter #(and (some? %) (-> % :order-id existing-order? not)))
                    (group-by :contract-id))]
    (if orders-changed
      orders
      (reduce
        (fn [orders mkt-id]
          (update
           orders
           mkt-id
           (fn [orders-by-contract-id]
             (->> (keys orders-by-contract-id)
                  (concat (keys to-add))
                  (into #{})
                  (map
                   (fn [contract-id]
                     (let [existing (get-in orders-by-contract-id [contract-id :orders])
                           add (get to-add contract-id)
                           rm (or (get to-remove contract-id) #{})]
                       [contract-id
                        {:valid? true
                         :orders (->> add
                                      (concat (filter #(->> % :order-id rm not) existing))
                                      (into []))}])))
                  (into {})))))
        prev-orders
        (->> submitted-orders
             (mapcat vals)
             (map :mkt-id)
             (into #{}))))))
