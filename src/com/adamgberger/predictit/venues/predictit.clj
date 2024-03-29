(ns com.adamgberger.predictit.venues.predictit
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.apis.predictit :as api])
  (:gen-class))

(defn- xform-result [f send-result]
  (fn [x]
    (if (instance? Throwable x)
      (send-result x)
      (let [v (try (f x)
                (catch Throwable t t))]
        (send-result v)))))

(defn- -current-available-balance
  "Gets the available cash for trading"
  [venue send-result]
  (api/get-balance
    (:auth venue)
    (xform-result
      #(get-in % [:balance :accountBalanceDecimal])
      send-result)))

(defn- -available-markets
  [venue send-result]
  (let [adapt-mkt (fn [m]
                    {:market-id (:marketId m)
                     :market-name (:marketName m)
                     :market-url (str "https://www.predictit.org/markets/detail/" (:marketId m) "/" (:marketUrl m))
                     :total-trades (:totalTrades m)
                     :total-shares-traded (:totalSharesTraded m)
                     :status (if (= (:status m) "Open") :open :closed)})]
    (api/get-markets
      (:auth venue)
      (xform-result
        #(->> %
              :markets
              (mapv adapt-mkt))
        send-result))))

(def side-by-trade-type
  (utils/rev-assoc api/numeric-trade-types))

(defn- -positions
  [venue send-result]
  (let [adapt-contract (fn [mkt-id c]
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
                                     (mapv (partial adapt-contract mkt-id)))})]
    (api/get-positions
      (:auth venue)
      (xform-result
        #(->> %
              :positions
              (mapv adapt-pos))
        send-result))))

(defn- -order-book
  [venue market-id full-market-url contract-id send-result]
  (letfn [(adapt-order-book [{:keys [order-book]}]
            {:yes {:buy (->> order-book
                             :yesOrders
                             (map
                              (fn [{:keys [contractId costPerShareYes quantity]}]
                                {:contract-id contractId
                                 :price costPerShareYes
                                 :qty quantity}))
                             (sort-by :price)
                             (into []))
                   :sell (->> order-book
                              :noOrders
                              (map
                               (fn [{:keys [contractId costPerShareYes quantity]}]
                                 {:contract-id contractId
                                  :price costPerShareYes
                                  :qty quantity}))
                              (sort-by :price)
                              reverse
                              (into []))}
             :no {:buy (->> order-book
                            :noOrders
                            (map
                             (fn [{:keys [contractId costPerShareNo quantity]}]
                               {:contract-id contractId
                                :price costPerShareNo
                                :qty quantity}))
                            (sort-by :price)
                            (into []))
                  :sell (->> order-book
                             :yesOrders
                             (map
                              (fn [{:keys [contractId costPerShareNo quantity]}]
                                {:contract-id contractId
                                 :price costPerShareNo
                                 :qty quantity}))
                             (sort-by :price)
                             reverse
                             (into []))}})]
    (api/get-order-book
      (:auth venue)
      market-id
      full-market-url
      contract-id
      (xform-result
        adapt-order-book
        send-result))))

(defn- -orders
  [venue market-id full-market-url contract-id send-result]
  (api/get-orders
    (:auth venue)
    market-id
    full-market-url
    contract-id
    (xform-result
      #(->> %
            :orders
            (mapv
             (fn [o]
               {:order-id (:offerId o)
                :contract-id (:contractId o)
                :qty (:remainingQuantity o)
                :price (:pricePerShare o)
                :trade-type (-> o :tradeType side-by-trade-type)
                :created-at (:dateCreated o)
                :cancellable? (:allowCancel o)})))
      send-result)))

(defn- -contracts
  [venue market-id full-market-url send-result]
  (let [adapt-contract (fn [c]
                         {:contract-id (:contractId c)
                          :contract-name (:contractName c)
                          :date-opened (:dateOpened c)
                          :tradable? (and (:isActive c) (:isOpen c) (not (:isTradingSuspended c)))
                          :last-price (:lastTradePrice c)})]
    (api/get-contracts
      (:auth venue)
      market-id
      full-market-url
      (xform-result
        #(->> %
              :contracts
              (mapv adapt-contract))
        send-result))))

(defn- -submit-order
  [venue market-id contract-id trade-type qty price send-result]
  (api/submit-order
    (:auth venue)
    market-id
    contract-id
    trade-type
    qty
    price
    (xform-result
      #(let [order (get-in % [:order :offer])]
        {:order-id (:offerId order)
         :mkt-id market-id
         :contract-id (:contractId order)
         :price (:pricePerShare order)
         :qty (:remainingQuantity order)
         :trade-type (-> order :tradeType side-by-trade-type)
         :cancellable? (not (:isProcessed order))
         :created-at (:dateCreated order)})
      send-result)))

(defn- -cancel-order
  [venue market-id order-id send-result]
  (api/cancel-order (:auth venue) market-id order-id send-result))

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
  [email pwd send-result]
  (let [cached-auth (-get-cached-auth email)]
    (if (some? cached-auth)
      (do (l/log :info "Using cached auth for predictit")
          (send-result cached-auth))
      (api/auth
        email
        pwd 
        #(do (spit auth-cache-fname (pr-str %))
             (send-result %))))))

(defn creds->auth [creds send-result]
  (-get-auth (:email creds) (:pwd creds) send-result))

(defmacro with-reauth [creds auth invocation]
  (let [send-result (last invocation)
        wrapper-sym (gensym)]
    `(let [~wrapper-sym (fn [res#] (if (and (instance? Throwable res#)
                                       (= (-> res# ex-data :status) 401))
                                (do (creds->auth
                                      ~creds
                                      (fn [a#] (swap! ~auth (constantly a#))))
                                    ~(concat
                                      (butlast invocation)
                                      [send-result]))
                                (~send-result res#)))]
        ~(concat (butlast invocation) [wrapper-sym]))))
                                   
(defn make-venue
  "Creates a venue"
  [creds send-result]
  (creds->auth
    creds
    #(let [auth (atom %)]
      (async/go-loop []
        (let [c (async/chan 1)]
          (async/<! (async/timeout (* 5 60 1000)))
          (api/ping (:auth @auth) (fn [_] (async/>! c 1)))
          (async/<! c)
          (recur)))
      (send-result
        (reify v/Venue
          (id [this] ::predictit)
          (current-available-balance [this send-result]
            (with-reauth creds auth (-current-available-balance @auth send-result)))
          (available-markets [this send-result]
            (with-reauth creds auth (-available-markets @auth send-result)))
          (positions [this send-result]
            (with-reauth creds auth (-positions @auth send-result)))
          (contracts [this market-id full-market-url send-result]
            (with-reauth creds auth (-contracts @auth market-id full-market-url send-result)))
          (order-book [this market-id full-market-url contract-id send-result]
            (with-reauth creds auth (-order-book @auth market-id full-market-url contract-id send-result)))
          (orders [this market-id full-market-url contract-id send-result]
            (with-reauth creds auth (-orders @auth market-id full-market-url contract-id send-result)))
          (submit-order [venue market-id contract-id trade-type qty price send-result]
            (with-reauth creds auth (-submit-order @auth market-id contract-id trade-type qty price send-result)))
          (cancel-order [venue market-id order-id send-result]
            (with-reauth creds auth (-cancel-order @auth market-id order-id send-result))))))))
