(ns com.adamgberger.predictit.venues.predictit
  (:require [com.adamgberger.predictit.venues.venue :as v]
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
    (let [adapt-contract (fn [c]
                            {:contract-id (:contractId c)
                             :name (:contractName c)
                             :started-at (:startDate c)
                             :ends-at (:endDate c)
                             :vol (:totalTrades c)
                             :last-trade (:lastTradePrice c)
                             :max-val (:maxShareValue c)})
          adapt-mkt (fn [m]
                        {:market-id (:marketId m)
                         :status (:status m)
                         :contracts (map adapt-contract (:contracts m))})
          mkts (api/get-markets (:auth venue))]
        (->> mkts
             :markets
             (map adapt-mkt)
             (filter (comp #{"Open"} :status)))))

(def side-by-trade-type
    (utils/rev-assoc api/numeric-trade-types))

(defn- -positions
    [venue]
    (let [portfolio (api/get-positions (:auth venue))
          adapt-contract (fn [c]
                            {:contract-id (:contractId c)
                             :side (-> c
                                       :userPrediction
                                       :side-by-trade-type
                                       name
                                       (.contains "yes")
                                       (if :yes :no))
                             :qty (:userQuantity c)
                             :orders {
                                 :buy (:userOpenOrdersBuyQuantity c)
                                 :sell (:userOpenOrdersSellQuantity c)}})
          adapt-pos (fn [mkt]
                        {:market-id (:marketId mkt)
                         :contracts (map adapt-contract (:marketContracts mkt))})]
        (->> portfolio
             :positions
             (map adapt-pos))))

(defn make-venue
    "Creates a venue"
    [creds]
    (let [auth (apply api/auth (map creds [:email :pwd]))]
        (reify v/Venue
            (id [this] ::predictit)
            (current-available-balance [this] (-current-available-balance auth))
            (available-markets [this] (-available-markets auth))
            (positions [this] (-positions auth)))))