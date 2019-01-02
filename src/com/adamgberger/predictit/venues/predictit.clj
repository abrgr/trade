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
          adapt-contract (fn [c]
                            {:contract-id (:contractId c)
                             :side (-> c
                                       :userPrediction
                                       side-by-trade-type
                                       name
                                       (.contains "yes")
                                       (if :yes :no))
                             :qty (:userQuantity c)
                             :orders {
                                 :buy (:userOpenOrdersBuyQuantity c)
                                 :sell (:userOpenOrdersSellQuantity c)}})
          adapt-pos (fn [mkt]
                        {:market-id (:marketId mkt)
                         :contracts (->> mkt
                                         :marketContracts
                                         (map adapt-contract)
                                         (into []))})]
        (->> portfolio
             :positions
             (map adapt-pos)
             (into []))))

(defn- -monitor-order-book
    [venue market-id full-market-url contract-id on-update continue-monitoring]
    (async/go
        (api/monitor-order-book (:auth venue) market-id full-market-url contract-id on-update continue-monitoring)))

(defn- -contracts
    [venue market-id full-market-url]
    (let [resp (api/get-contracts venue market-id full-market-url)
          adapt-contract (fn [c]
                            {:contract-id (:contractId c)
                             :contract-name (:contractName c)
                             :date-opened (:dateOpened c)
                             :is-active (:isActive c)
                             :is-open (:isOpen c)
                             :is-trading-suspended (:isTradingSuspended c)})]
        (->> resp
             :contracts
             (map adapt-contract)
             (into []))))

(defn make-venue
    "Creates a venue"
    [creds]
    (let [auth (apply api/auth (map creds [:email :pwd]))]
        (reify v/Venue
            (id [this] ::predictit)
            (current-available-balance [this] (-current-available-balance auth))
            (available-markets [this] (-available-markets auth))
            (positions [this] (-positions auth))
            (contracts [this market-id full-market-url] (-contracts auth market-id full-market-url))
            (monitor-order-book [this market-id market-name contract-id on-update continue-monitoring]
                (-monitor-order-book auth market-id market-name contract-id on-update continue-monitoring)))))