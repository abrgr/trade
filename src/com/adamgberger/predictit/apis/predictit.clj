(ns com.adamgberger.predictit.apis.predictit
  (:require [clj-http.client :as h]
            [clj-http.conn-mgr :as conn-mgr]
            [clj-http.cookies :as cookies]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(def -cfg
  {:site-url "https://www.predictit.org"
   :api-url "https://www.predictit.org/api"
   :apiKey "AIzaSyDXdDCHMMqgwx2RS1ORhZHeczBgAHyJ3oA"
   :authDomain "predictit-f497e.firebaseapp.com"
   :databaseURL "https://predictit-f497e.firebaseio.com"
   :projectId "predictit-f497e"
   :messagingSenderId "76708845910"})

(defn predictit-firebase-url [path]
  (str (:databaseURL -cfg) path))

(defn predictit-api-url [path]
  (str (:api-url -cfg) path))

(defn predictit-site-url [path]
  (str (:site-url -cfg) path))

(def cm (conn-mgr/make-reusable-conn-manager {:timeout 10 :threads 30}))

(def async-cm (conn-mgr/make-reuseable-async-conn-manager {:timeout 10}))

(def cs (cookies/cookie-store))

(def ^:private default-headers
  {"User-Agent" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
   "Accept-Language" "en-US,en;q=0.9"
   "Accept" "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
   "Cache-Control" "no-cache"
   "Pragma" "no-cache"})

(def ^:private default-post-headers
  (assoc default-headers "Accept" "application/json, text/plain, */*"))

(defn http-post [url opts send-result send-error]
  (let
   [def-opts {:connection-manager async-cm
              :cookie-store cs
              :headers default-post-headers}
    merged-opts (utils/merge-deep def-opts opts {:async? true})]
    (h/post url merged-opts send-result send-error)))

(defn http-get
  ([url] (http-get url {}))
  ([url opts]
   (let [def-opts {:connection-manager cm
                   :cookie-store cs
                   :headers default-headers}
         merged-opts (utils/merge-deep def-opts opts)]
     (h/get url merged-opts)))
  ([url opts send-result send-error]
   (let [def-opts {:connection-manager async-cm
                   :cookie-store cs
                   :headers default-headers}
         merged-opts (utils/merge-deep def-opts opts {:async? true})]
     (h/get url merged-opts send-result send-error))))

(defn from-page [page]
    ; (http-get page) ; try to emulate a browser
  {"Referrer" (predictit-site-url "/dashboard")})

(defn with-auth [auth]
  {"Authorization" (str "Bearer " (:access_token auth))})

(defn json-req []
  {"Accept" "application/json, text/plain, */*"})

(defn resp-from-json
  ([resp] (resp-from-json resp {}))
  ([resp value-fns]
   (-> resp
       :body
       (utils/read-json value-fns))))

(defn each-line [is f value-fns continue-monitoring]
  (with-open [^java.io.BufferedReader rdr (io/reader is)]
    (loop [^String evt-line (.readLine rdr)
           ^String data-line (.readLine rdr)]
      (let [evt (-> evt-line
                    (utils/split-first ":")
                    ^String (second)
                    .trim
                    .toLowerCase)
            payload (-> data-line
                        (utils/split-first ":")
                        second
                        .trim
                        (utils/read-json value-fns))]
        (cond
          (= evt "keep-alive")
          nil
          (= evt "put")
          (do
            (when (not= (:path payload) "/")
              (throw (ex-info "Bad payload" {:should-retry false :payload payload})))
            (f (:data payload)))
          :else
          (do
            (l/log :warn "Unhandled event type" {:evt evt :payload payload})
            (throw (ex-info "Unhandled event type" {:should-retry true}))))
        (.readLine rdr) ; eat blank line
        (when (continue-monitoring)
          (recur (.readLine rdr) (.readLine rdr)))))))

(defn auth
  "Authenticates with predictit.
     Returns something like:
     {:auth {:access_token ...
             :token_type bearer
             :expires_in 25199
             :refresh_token ...
             :as:device_id ...
             :userName adam.g.berger@gmail.com
             :.issued 2018-12-30T18:29:55.0000000+00:00
             :.expires 2018-12-31T01:29:55.0000000+00:00}}"
  [email pwd send-result]
  (let [params {:headers (from-page (predictit-site-url ""))
                :form-params
                {:email email
                 :password pwd
                 :grant_type "password"
                 :rememberMe false}}
        value-fns {}]
    (http-post
      (predictit-api-url "/Account/token")
      params
      #(send-result
        (if (-> % :body nil?)
          (ex-info "Bad auth response" {:resp %})
          {:auth (resp-from-json % value-fns)}))
      send-result)))

(defn ping
  "Keeps the session alive.
   Returns nil"
  [auth send-result]
  (let [headers (from-page (predictit-site-url "/dashboard"))
        url (predictit-site-url (str "/signalr/ping?bearer=" (:access_token auth) "&_=" (inst-ms (java.time.Instant/now))))]
    (http-get
      url
      {:headers headers})
      (fn [_] (send-result nil))
      send-result))

(defn get-balance
  "Retrieves current balance.
     Returns something like:
     {:balance {:accountBalance $389.40
                :accountBalanceDecimal 389.404
                :portfolioBalance $149.40
                :portfolioBalanceDecimal 149.4}}"
  [auth send-result]
  (let [headers (->> (from-page (predictit-site-url "/dashboard"))
                     (merge (json-req))
                     (merge (with-auth auth)))
        url (predictit-api-url "/User/Wallet/Balance")
        value-fns {:portfolioBalanceDecimal utils/to-decimal
                   :accountBalanceDecimal utils/to-decimal}]
    (http-get
      url
      {:headers headers}
      #(send-result
          (if (-> % :body nil?)
            (ex-info "Bad wallet response" {:resp %})
            {:balance (resp-from-json % value-fns)}))
      send-result)))

(defn get-markets
  "Retrieves all markets.
     Returns something like:
     {:markets [
        {:isMarketWatched false
         :userHasOwnership false
         :marketImageUrl \"https://az620379.vo.msecnd.net/images/Markets/50fede7c-7b46-4fe3-b431-911baecd6f00.jpg\"
         :totalSharesTraded 19246761
         :totalTrades 245211
         :status \"Open\"
         :marketName \"GOP Senate seats after midterms?\"
         :marketId 2891
         :marketUrl \"How-many-Senate-seats-will-the-GOP-hold-after-2018-midterms\"
         :contracts [{:bestNoPrice 0.26
                     :maxShareValue 1.0
                     :startDate \"2017-01-19T19:34:37.56\"
                     :hasTodaysChange false
                     :todaysChangePercentage 0.0
                     :contractName 52
                     :contractId 5166
                     :lastTradePrice 0.76
                     :lastClosePrice 0.77
                     :totalTrades 31038
                     :endDate \"01/04/2019 11:59 PM (ET)\"
                     :bestYesPrice 0.77
                     :todaysChange 0.0
                     :contractImageUrl \"https://az620379.vo.msecnd.net/images/Contracts/6a701e4a-ea57-40c0-a609-606cde281640.jpg\"}
                     ...]
        }
        ...]}}"
  [auth send-result]
  (let [headers (->> (from-page (predictit-site-url "/markets"))
                     (merge (json-req))
                     (merge (with-auth auth)))
        value-fns {:endDate utils/parse-localish-datetime
                   :startDate utils/parse-isoish-datetime}
        ch (async/chan)
        handle-resp (fn [page markets resp]
                      (if (-> resp :body nil?)
                        (do (async/close! ch)
                            (send-result (ex-info "Bad markets response" {:resp resp})))
                        (let [{total-pages :totalPages
                               page-markets :markets} (resp-from-json resp value-fns)]
                          (if (< page total-pages)
                            (async/put! ch {:page (inc page)
                                            :markets (concat markets page-markets)})
                            (do (async/close! ch)
                                (send-result {:markets (concat markets page-markets)}))))))]
    (async/put! ch {:page 1 :markets []})
    (async/go-loop []
      (async/<! (async/timeout (+ 200 (rand-int 200)))) ; just to play nice...
      (let [{:keys [page markets] :as c} (async/<! ch)]
        (when (some? c)
          (let [qs (str "?itemsPerPage=20&page=" page "&filterIds=&sort=traded&sortParameter=ALL")
                url (predictit-api-url (str "/Browse/FilteredMarkets/0" qs))]
            (http-get
              url
              {:headers headers}
              (partial handle-resp page markets)
              #(do (async/close! ch)
                   (send-result %))))
          (recur))))))

(def numeric-trade-types
  {:buy-no   0
   :buy-yes  1
   :sell-no  2
   :sell-yes 3})

(defn submit-order
  "Submits an order.
     Returns something like:
     {:order
        {:offer {
            :offerId 22752136
            :contractId: 13695
            :pricePerShare: 0.04
            :quantity: 1
            :remainingQuantity: 1
            :tradeType: 1
            :dateCreated: #inst 2018-12-31T04:21:13.6933278Z
            :isProcessed: false}
        :contractId: 13695
        :longName: \"Will Trump's 538 job approval index for December 31 be 41.7% - 41.9%?\"
        :marketId: 5122
        :imageName: \"https://az620379.vo.msecnd.net/images/Contracts/1a9a6185-bf1d-4321-8316-d02f217b76e0.jpg\"
        :isEngineBusy: false
        :isEngineBusyMessage: null}}"
  [auth market-id contract-id trade-type quantity price send-result]
  (let [headers (->> (from-page (predictit-site-url "/dashboard"))
                     (merge (json-req))
                     (merge (with-auth auth)))
        params {:headers headers
                :form-params {:quantity (int quantity)
                              :pricePerShare (int (* 100 price))
                              :contractId contract-id
                              :tradeType (trade-type numeric-trade-types)}}]
    (http-post
      (predictit-api-url "/Trade/SubmitTrade")
      params
      #(send-result
        (if (-> % :body nil?)
          (ex-info "Bad submit-order response" {:resp %})
          {:order (resp-from-json %)}))
      send-result)))

(defn cancel-order
  "Cancels an order.
     Returns something like:
     {:success true}"
  [auth market-id order-id send-result]
  (let [headers (->> (from-page (predictit-site-url "/dashboard"))
                     (merge (json-req))
                     (merge (with-auth auth)))
        params {:headers headers}]
    (http-post
      (predictit-api-url (str "/Trade/CancelOffer/" order-id))
      params
      #(send-result {:success true})
      send-result)))

(defn get-orders
  "Retrieves current orders.
     Returns something like this:
     {:orders [{:offerId 23041022,
                :contractId 13815,
                :pricePerShare 0.89,
                :quantity 50,
                :remainingQuantity 50,
                :tradeType 2,
                :dateCreated #inst\"2019-01-14T20:56:41.56\",
                :isProcessed true}]}"
  [auth market-id full-market-url contract-id send-result]
  (let [headers (->> (from-page full-market-url)
                     (merge (json-req))
                     (merge (with-auth auth)))
        value-fns {:dateCreated utils/parse-isoish-datetime
                   :pricePerShare utils/to-price}
        url (predictit-api-url (str "/Profile/contract/" contract-id "/Offers"))]
    (http-get
      url
      {:headers headers}
      #(send-result
        (if (-> % :body nil?)
          (ex-info "Bad orders response" {:resp %})
          (let [json (resp-from-json % value-fns)
                allow-cancel (:allowCancel json)]
            {:orders (map
                      (partial merge {:allowCancel allow-cancel})
                      (:offers json))})))
      send-result)))

(defn get-order-book
  "Retrieves current order book.
     Returns something like this:
     {:order-book {:contract-id 1234
                   :noOrders []
                   :yesOrders [
                       {:contract-id
                        :costPerShareNo 0.99
                        :costPerShareYes 0.01
                        :pricePerShare 0.01
                        :quantity 4453
                        :tradeType 0}
                        ...]}"
  [auth market-id full-market-url contract-id send-result]
  (let [headers (->> (from-page full-market-url)
                     (merge (json-req))
                     (merge (with-auth auth)))
        value-fns {:userInvestment utils/to-decimal
                   :userMaxPayout utils/to-decimal
                   :userAveragePricePerShare utils/to-decimal
                   :pricePerShare utils/to-price
                   :costPerShareNo utils/to-price
                   :costPerShareYes utils/to-price}
        url (predictit-api-url (str "/Trade/" contract-id "/OrderBook"))]
    (http-get
      url
      {:headers headers}
      #(if (-> % :body nil?)
        (ex-info "Bad order book response" {:resp %})
        {:order-book (merge (resp-from-json % value-fns) {:contract-id contract-id})})
      send-result)))

(defn get-positions
  "Retrieves current holdings.
     Returns something like this:
     {:portfolio [{:isActive true
                   :isOpen true <- indicates available for trading
                   :marketId 5130
                   :marketType 3 <- 3 = multiple contracts; 0 = single contract
                   :marketName \"What will Trump's ...\"
                   :userInvestment: 10 <- in dollars
                   :userMaxPayout 46 <- in dollars
                   :marketContracts [{:contractId 13733
                                      :contractIsActive true
                                      :contractIsOpen true
                                      :contractName \"42.6% - 42.9%\"
                                      :userPrediction 1 <- according to numeric-trade-types
                                      :userQuantity 50
                                      :userAveragePricePerShare 0.2
                                      :userOpenOrdersBuyQuantity 0
                                      :userOpenOrdersSellQuantity 0}]}]}"
  [auth send-result]
  (let [headers (->> (from-page (predictit-site-url "/markets"))
                     (merge (json-req))
                     (merge (with-auth auth)))
        value-fns {:userInvestment utils/to-decimal
                   :userMaxPayout utils/to-decimal
                   :userAveragePricePerShare utils/to-decimal}
        qs (str "?sort=traded&sortParameter=ALL")
        url (predictit-api-url (str "/Profile/Shares" qs))]
    (http-get
      url
      {:headers headers}
      #(send-result
        (if (-> % :body nil?)
          (ex-info "Bad portfolio response" {:resp %})
          (let [{suspended? :isTradingSuspended
                 suspension-msg :isTradingSuspendedMessage
                 mkts :markets} (resp-from-json % value-fns)]
            (if suspended?
              (ex-info "TRADING SUSPENDED" {:suspension-msg suspension-msg})
              {:positions mkts}))))
      send-result)))

(defn get-contracts
  "Retrieves contracts for a market.
     Returns something like:
     {:contracts [{:contractId 5174,
                   :contractName \"60 or more\",
                   :marketId 2891,
                   :marketName \"How many Senate seats will the GOP hold after 2018 midterms?\",
                   :contractImageUrl \"https://az620379.vo.msecnd.net/images/Contracts/cc8f1b1a-4ac1-426a-a706-b7a26ed45d42.jpg\",
                   :contractImageSmallUrl \"https://az620379.vo.msecnd.net/images/Contracts/small_cc8f1b1a-4ac1-426a-a706-b7a26ed45d42.jpg\",
                   :isActive true,
                   :isOpen true,
                   :lastTradePrice 0.01,
                   :lastClosePrice 0.01,
                   :bestYesPrice 0.01,
                   :bestYesQuantity 842,
                   :bestNoPrice null,
                   :bestNoQuantity 0,
                   :userPrediction 0,
                   :userQuantity 0,
                   :userOpenOrdersBuyQuantity 0,
                   :userOpenOrdersSellQuantity 0,
                   :userAveragePricePerShare 0.000000,
                   :isTradingSuspended false,
                   :dateOpened \"2017-01-19T19:34:37.56\",
                   :hiddenByDefault false,
                   :displayOrder 0}
                  ...]}"
  [auth market-id full-market-url send-result]
  (let [headers (->> (from-page full-market-url)
                     (merge (json-req))
                     (merge (with-auth auth)))
        value-fns {:bestYesPrice utils/to-price
                   :bestNoPrice utils/to-price
                   :lastTradePrice utils/to-price
                   :lastClosePrice utils/to-price
                   :userAveragePricePerShare utils/to-decimal
                   :dateOpened utils/parse-isoish-datetime}]
    (http-get
      (predictit-api-url (str "/Market/" market-id "/Contracts"))
      {:headers headers}
      #(send-result
        (if (-> % :body nil?)
          (ex-info "Bad contracts response" {:resp %})
          {:contracts (resp-from-json % value-fns)}))
      send-result)))
