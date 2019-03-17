(ns com.adamgberger.predictit.venues.predictit
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.apis.predictit :as api])
  (:gen-class))

(defn- -current-available-balance
  "Gets the available cash for trading"
  [venue]
  (let [bal (api/get-balance (:auth venue))]
    (get-in bal [:balance :accountBalanceDecimal])))

(defn- -available-markets
  [venue]
  (let [adapt-mkt (fn [m]
                    {:market-id (:marketId m)
                     :market-name (:marketName m)
                     :market-url (str "https://www.predictit.org/markets/detail/" (:marketId m) "/" (:marketUrl m))
                     :total-trades (:totalTrades m)
                     :total-shares-traded (:totalSharesTraded m)
                     :status (if (= (:status m) "Open") :open :closed)})
        mkts (api/get-markets (:auth venue))]
    (->> mkts
         :markets
         (map adapt-mkt)
         (into []))))

(def side-by-trade-type
  (utils/rev-assoc api/numeric-trade-types))

(defn- -positions
  [venue]
  (let [portfolio (api/get-positions (:auth venue))
        adapt-contract (fn [mkt-id c]
                         {:contract-id (:contractId c)
                          :market-id mkt-id
                          :tradable? (and (:contractIsOpen c) (:contractIsActive c))
                          :side (-> c
                                    :userPrediction
                                    side-by-trade-type
                                    name
                                    (.contains "yes")
                                    (if :yes :no))
                          :qty (:userQuantity c)
                          :avg-price-paid (:userAveragePricePerShare c)
                          :orders {:buy (:userOpenOrdersBuyQuantity c)
                                   :sell (:userOpenOrdersSellQuantity c)}})
        adapt-pos (fn [{mkt-id :marketId :as mkt}]
                    {:market-id mkt-id
                     :contracts (->> mkt
                                     :marketContracts
                                     (map (partial adapt-contract mkt-id))
                                     (into []))})]
    (->> portfolio
         :positions
         (map adapt-pos)
         (into []))))

(defn- -monitor-order-book
  [venue market-id full-market-url contract-id]
  (letfn [(adapt-order-book [{:keys [order-book]}]
            {:yes {:buy (->> (:yesOrders order-book)
                             (map
                              (fn [{:keys [contractId costPerShareYes quantity]}]
                                {:contract-id contractId
                                 :price costPerShareYes
                                 :qty quantity}))
                             (sort-by :price)
                             (into []))
                   :sell (->> (:noOrders order-book)
                              (map
                               (fn [{:keys [contractId costPerShareYes quantity]}]
                                 {:contract-id contractId
                                  :price costPerShareYes
                                  :qty quantity}))
                              (sort-by :price)
                              reverse
                              (into []))}
             :no {:buy (->> (:noOrders order-book)
                            (map
                             (fn [{:keys [contractId costPerShareNo quantity]}]
                               {:contract-id contractId
                                :price costPerShareNo
                                :qty quantity}))
                            (sort-by :price)
                            (into []))
                  :sell (->> (:yesOrders order-book)
                             (map
                              (fn [{:keys [contractId costPerShareNo quantity]}]
                                {:contract-id contractId
                                 :price costPerShareNo
                                 :qty quantity}))
                             (sort-by :price)
                             reverse
                             (into []))}})]
    (map
     adapt-order-book
     (api/monitor-order-book (:auth venue) market-id full-market-url contract-id))))

(defn- -orders
  [venue market-id full-market-url contract-id]
  (->> (api/get-orders (:auth venue) market-id full-market-url contract-id)
       :orders
       (map
        (fn [o]
          {:order-id (:offerId o)
           :contract-id (:contractId o)
           :qty (:remainingQuantity o)
           :price (:pricePerShare o)
           :trade-type (-> o :tradeType side-by-trade-type)
           :created-at (:dateCreated o)
           :cancellable? (:allowCancel o)}))
       (into [])))

(defn- -contracts
  [venue market-id full-market-url]
  (let [resp (api/get-contracts (:auth venue) market-id full-market-url)
        adapt-contract (fn [c]
                         {:contract-id (:contractId c)
                          :contract-name (:contractName c)
                          :date-opened (:dateOpened c)
                          :tradable? (and (:isActive c) (:isOpen c) (not (:isTradingSuspended c)))
                          :last-price (:lastTradePrice c)})]
    (->> resp
         :contracts
         (map adapt-contract)
         (into []))))

(defn- -submit-order
  [venue market-id contract-id trade-type qty price]
  (let [resp (api/submit-order (:auth venue) market-id contract-id trade-type qty price)
        order (get-in resp [:order :offer])]
    {:order-id (:offerId order)
     :contract-id (:contractId order)
     :price (:pricePerShare order)
     :qty (:remainingQuantity order)
     :trade-type (-> order :tradeType side-by-trade-type)
     :cancellable? (not (:isProcessed order))
     :created-at (:dateCreated order)}))

(defn- -cancel-order
  [venue market-id order-id]
  (api/cancel-order (:auth venue) market-id order-id))

(def auth-cache-fname ".predictit-auth")

(defn- -get-cached-auth [email]
  (letfn [(if-auth-valid [auth]
            (if (and (some? auth)
                     (some? (-> auth :auth :access_token))
                     (= (-> auth :auth :userName) email)
                     (<= (compare (java.time.Instant/now) (-> auth :auth :.expires utils/parse-offset-datetime)) 0))
              auth
              nil))]
    (if (->> auth-cache-fname clojure.java.io/file .exists)
      (-> auth-cache-fname
          slurp
          clojure.edn/read-string
          if-auth-valid)
      (do
        (l/log :info "auth cache does not exist" {:fname auth-cache-fname})
        nil))))

(defn- -get-auth
  [email pwd]
  (let [cached-auth (-get-cached-auth email)]
    (if (some? cached-auth)
      (do (l/log :info "Using cached auth for predictit")
          cached-auth)
      (let [new-auth (api/auth email pwd)]
        (spit auth-cache-fname (pr-str new-auth))
        new-auth))))

(defn make-venue
  "Creates a venue"
  [creds]
  (let [auth (apply -get-auth (map creds [:email :pwd]))]
    (reify v/Venue
      (id [this] ::predictit)
      (current-available-balance [this] (-current-available-balance auth))
      (available-markets [this] (-available-markets auth))
      (positions [this] (-positions auth))
      (contracts [this market-id full-market-url] (-contracts auth market-id full-market-url))
      (monitor-order-book [this market-id market-name contract-id]
        (-monitor-order-book auth market-id market-name contract-id))
      (orders [this market-id full-market-url contract-id]
        (-orders auth market-id full-market-url contract-id))
      (submit-order [venue market-id contract-id trade-type qty price]
        (-submit-order auth market-id contract-id trade-type qty price))
      (cancel-order [venue market-id order-id]
        (-cancel-order auth market-id order-id)))))