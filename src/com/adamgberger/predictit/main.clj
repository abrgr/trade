(ns com.adamgberger.predictit.main
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.venues :as vs]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.strategies :as strats]
            [com.adamgberger.predictit.inputs :as inputs]
            [com.adamgberger.predictit.estimators :as estimators])
  (:gen-class))

(defn get-available-markets-chan [venue]
  (async/thread (v/available-markets venue)))

(defn maintain-venue-state [key interval-ms updater state end-chan]
  ; we kick off a go-routine that updates each venue's state
  ; at key with the result of (updater venue)
  ; every interval-ms and only stops when end-chan is closed

  (l/log :info "Starting venue maintenance" {:key key})

  (let [{:keys [venues venue-state]} state
        upd (fn [venue-state id val]
              (assoc-in venue-state [id key] val))
        handler #(doseq [venue venues]
                    (let [val (updater venue)
                          id (v/id venue)]
                      (send-off venue-state upd id val)))]
    (utils/repeatedly-until-closed
      handler
      600000
      #(l/log :warn "Stopping venue maintenance" {:key key})
      end-chan)))

(defn maintain-markets [state end-chan]
  ; we kick off a go-routine that polls all available markets in each venue
  ; and updates venue-state with the results.

  (letfn [(get-mkts [venue]
            (let [mkts (v/available-markets venue)]
              (l/log :info "Found markets for venue" {:venue-id (v/id venue)
                                                      :market-count (count mkts)})
              mkts))]
    (maintain-venue-state :mkts 600000 get-mkts state end-chan)))

(defn start-strategies [cfg state end-chan]
  (reduce
    (fn [strats next]
      (merge strats (next cfg state end-chan)))
    {}
    (strats/all)))

(defn maintain-balances [state end-chan]
  ; we kick off a go-routine that polls balance in each venue
  ; and updates venue-state with the results.

  (letfn [(get-bal [venue]
            (let [bal (v/current-available-balance venue)]
              (l/log :info "Found balance for venue" {:venue-id (v/id venue)
                                                      :balance bal})
              bal))]
    (maintain-venue-state :bal 600000 get-bal state end-chan)))

(defn maintain-positions [state end-chan]
  ; we kick off a go-routine that polls balance in each venue
  ; and updates venue-state with the results.

  (letfn [(get-pos [venue]
            (let [pos (v/positions venue)]
              (l/log :info "Found positions for venue" {:venue-id (v/id venue)
                                                        :pos pos})
              pos))]
    (maintain-venue-state :pos 60000 get-pos state end-chan)))

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
      20000
      #(l/log :warn "Stopping state watchdog")
      end-chan)))

(defn update-order-book [state venue-id market-id contract-id val]
  (l/log :info "Received order book update" {:venue-id venue-id
                                             :market-id market-id
                                             :contract-id contract-id})
  (send
    (:venue-state state)
    #(let [last-price (get-in % [venue-id :contracts market-id contract-id :last-price])]
      (assoc-in
        %
        [venue-id :order-books market-id contract-id]
        (merge {:last-price last-price} val)))))

(defn all-order-book-reqs [venue-state venue-id]
  (->> (get-in venue-state [venue-id :req-order-books])
       vals
       (apply concat)
       (into #{})))

(defn continue-monitoring-order-book [state venue-id market-id]
  (let [venue-state @(:venue-state state)
        req-books (all-order-book-reqs venue-state venue-id)]
    (->> req-books
         (filter #(= market-id (:market-id %)))
         empty?
         not)))

(defn monitor-order-book [state end-chan venue-id market-id contract-id order-book-updates]
  (async/go-loop []
    (update-order-book state venue-id market-id contract-id (first order-book-updates))
    (let [interval 30000
          keep-going? (async/alt!
                        end-chan false
                        (async/timeout interval) true)]
      (if (and (continue-monitoring-order-book state venue-id market-id)
               keep-going?)
        (recur)
        (l/log :warn "Stopping order book monitor" {:market-id market-id
                                                    :contract-id contract-id})))))

(defn update-contracts [state venue-id market-id market-url]
  (let [venue (->> state
                    :venues
                    (filter #(= venue-id (v/id %)))
                    first)
        contracts (v/contracts venue market-id market-url)]
    (doseq [contract contracts]
      (let [contract-id (:contract-id contract)]
        (send
          (:venue-state state)
          #(assoc-in % [venue-id :contracts market-id contract-id] contract))))
    contracts))

(defn monitor-market-contracts [state end-chan venue-id market-id market-url]
  (async/go-loop []
    (update-contracts state venue-id market-id market-url)
    (let [interval 30000
          keep-going? (async/alt!
                        end-chan false
                        (async/timeout interval) true)]
      (if (and (continue-monitoring-order-book state venue-id market-id)
               keep-going?)
        (recur)
        (l/log :warn "Stopping market contracts monitor" {:market-id market-id})))))

(defn maintain-order-book [state end-chan venue-id venue req]
  (l/log :info "Starting watch of order book" {:venue-id venue-id
                                               :req req})
  (let [{:keys [market-id market-url]} req
        contracts (update-contracts state venue-id market-id market-url)]
    (monitor-market-contracts state end-chan venue-id market-id market-url)
    (doseq [contract contracts]
      (let [contract-id (:contract-id contract)]
        (->> (v/monitor-order-book
              venue
              market-id
              market-url
              contract-id)
            (monitor-order-book state end-chan venue-id market-id contract-id))))))

(defn maintain-order-books [state end-chan]
  ; monitor state -> venue-state -> venue-id -> :req-order-books -> strat-id for sets of maps like this:
  ; {:market-id x :market-name x :contract-id y}
  (add-watch
    (:venue-state state)
    ::maintain-order-books
    (fn [_ _ old new]
      (let [venue-ids (set (concat (keys old) (keys new)))]
        (doseq [venue-id venue-ids]
          (let [new-reqs (all-order-book-reqs new venue-id)
                old-reqs (all-order-book-reqs old venue-id)
                added (clojure.set/difference new-reqs old-reqs)
                venue (->> state
                           :venues
                           (filter #(= venue-id (v/id %)))
                           first)]
            (doseq [to-add added]
              (maintain-order-book state end-chan venue-id venue to-add))))))))

(defn orig-investment [positions]
  (->> positions
       (mapcat :contracts)
       (map #(* (:qty %) (:avg-price-paid %)))
       (reduce + 0.0)))

(defn mkt-ids-for-strat [venue-state venue-id strat-id]
  (->> venue-state
       venue-id
       :req-order-books
       strat-id
       (map :market-id)
       (into #{})))

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

(defn get-orders [state venue-id venue-state mkt-id]
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

(defn outstanding-orders-for-strat [state venue-id venue-state strat-id]
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

(defn desired-pos-for-req-pos [bankroll req-pos]
  {:contract-id (:contract-id req-pos)
   :target-price (:prob req-pos)
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

(defn orders-by-trade-type [orders]
  (reduce
    (fn [by-type order]
      (update by-type (:trade-type order) #(conj % order)))
    {:buy-yes []
     :sell-yes []
     :buy-no []
     :sell-no []}
    orders))

(defn adjust-desired-pos-for-actuals [desired-pos current-pos-by-contract-id outstanding-orders-by-contract-id]
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
                       still-valid-order? #(< (Math/abs (- (:price %) desired-price)) 0.05)
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

(defn run-executor [state end-chan]
  (let [venue-id (-> state :venues first v/id)]
    (utils/add-guarded-watch-in
        (:venue-state state)
        ::run-executor
        [venue-id :req-pos]
        not=
        (partial execute-trades state end-chan venue-id))))

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
        state (assoc-in state [:strats] (start-strategies (:strats cfg) state end-chan))]
    (run-executor state end-chan)
    (maintain-markets state end-chan)
    (maintain-balances state end-chan)
    (maintain-positions state end-chan)
    (maintain-order-books state end-chan)
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
