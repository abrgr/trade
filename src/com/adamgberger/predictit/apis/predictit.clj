(ns com.adamgberger.predictit.apis.predictit
  (:require [clj-http.client :as h]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
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

(def cm (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10 :threads 30}))

(def cs (clj-http.cookies/cookie-store))

(defn http-post [url opts]
    (let
        [def-opts {:connection-manager cm
                   :cookie-store cs
                   :headers {"User-Agent" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
                             "Accept-Language" "en-US,en;q=0.9"
                             "Accept" "application/json, text/plain, */*"
                             "Cache-Control" "no-cache"
                             "Pragma" "no-cache"}}
         merged-opts (utils/merge-deep def-opts opts)]
        (h/post url merged-opts)))

(defn http-get
    ([url] (http-get url {}))
    ([url opts]
        (let
            [def-opts {:connection-manager cm
                       :cookie-store cs
                       :headers {"User-Agent" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
                                 "Accept-Language" "en-US,en;q=0.9"
                                 "Accept" "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
                                 "Cache-Control" "no-cache"
                                 "Pragma" "no-cache"}}
            merged-opts (utils/merge-deep def-opts opts)]
            (h/get url merged-opts))))

(defn from-page [page]
    (http-get page) ; try to emulate a browser
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
    (let [rdr (io/reader is)]
        (loop [evt-line (.readLine rdr)
               data-line (.readLine rdr)]
            (let [evt (-> evt-line
                          (utils/split-first ":")
                          second
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
                    (and (= evt "put"))
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
    [email pwd]
    (l/with-log :debug (str "Authenticating with user [" email "]")
        (let [params {:headers (from-page (predictit-site-url ""))
                      :form-params
                        {:email email
                        :password pwd
                        :grant_type "password"
                        :rememberMe false}}
            resp (http-post (predictit-api-url "/Account/token") params)
            value-fns {:.issued utils/parse-offset-datetime
                       :.expires utils/parse-offset-datetime}]
            (when-not (-> resp :body some?)
                (throw (ex-info "Bad auth response" {:resp resp})))
            {:auth (resp-from-json resp)})))

(defn get-balance
    "Retrieves current balance.
     Returns something like:
     {:balance {:accountBalance $389.40
                :accountBalanceDecimal 389.404
                :portfolioBalance $149.40
                :portfolioBalanceDecimal 149.4}}"
    [auth]
    (l/with-log :debug "Getting balance"
        (let [headers (->> (from-page (predictit-site-url "/dashboard"))
                           (merge (json-req))
                           (merge (with-auth auth)))
            resp (http-get (predictit-api-url "/User/Wallet/Balance") {:headers headers})
            value-fns {:portfolioBalanceDecimal utils/to-decimal
                       :accountBalanceDecimal utils/to-decimal}]
            (when-not (-> resp :body some?)
                (throw (ex-info "Bad wallet response" {:resp resp})))
            {:balance (resp-from-json resp)})))

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
    [auth]
    (l/with-log :debug "Retrieving markets"
        (let [headers (->> (from-page (predictit-site-url "/markets"))
                           (merge (json-req))
                           (merge (with-auth auth)))
              value-fns {:endDate utils/parse-localish-datetime
                         :startDate utils/parse-isoish-datetime}]
            (loop [page 1
                   markets []]
                (Thread/sleep (+ 200 (rand-int 200))) ; just to play nice...
                (let [qs (str "?itemsPerPage=20&page=" page "&filterIds=&sort=traded&sortParameter=ALL")
                      url (predictit-api-url (str "/Browse/FilteredMarkets/0" qs))
                      resp (http-get url {:headers headers})]
                    (when-not (-> resp :body some?)
                        (throw (ex-info "Bad markets response" {:resp resp})))
                    (let [{total-pages :totalPages
                           page-markets :markets} (resp-from-json resp value-fns)]
                        (if (< page total-pages)
                            (recur (inc page) (concat markets page-markets))
                            {:markets (concat markets page-markets)})))))))

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
    [auth market-id contract-id trade-type quantity price]
    (l/with-log :info "Submitting trade"
        (let [headers (->> (from-page (predictit-site-url "/dashboard"))
                           (merge (json-req))
                           (merge (with-auth auth)))
            params {:headers headers
                    :form-params {:quantity quantity
                                  :pricePerShare price
                                  :contractId contract-id
                                  :tradeType (trade-type numeric-trade-types)}}
            resp (http-post (predictit-api-url "/Trade/SubmitTrade") params)]
            (when-not (-> resp :body some?)
                (throw (ex-info "Bad submit-order response" {:resp resp})))
            {:order (resp-from-json resp)})))

(defn cancel-order
    "Cancels an order.
     Returns something like:
     {:success true}"
    [auth market-id order-id]
    (l/with-log :info "Canceling trade"
        (let [headers (->> (from-page (predictit-site-url "/dashboard"))
                           (merge (json-req))
                           (merge (with-auth auth)))
            params {:headers headers}
            resp (http-post (predictit-api-url "/Trade/CancelOffer/" order-id) params)]
            {:success true})))

(defn monitor-order-book
    "Monitors the order book for a contract.
     Invokes on-update with each order book update.  Checks (continue-monitoring market-id contract-id) at each iteration.
     Updates look like:
     {:noOrders []
      :timestamp #inst 1546213580.64377
      :yesOrders [
        {:costPerShareNo 0.99
         :costPerShareYes 0.01
         :pricePerShare 0.01
         :quantity 4453
         :tradeType 0}
        ...]}"
    [auth market-id full-market-url contract-id on-update continue-monitoring]
    (l/with-log :debug "Monitor order-book"
        (let [url (predictit-firebase-url (str "/contractOrderBook/" contract-id ".json"))
              headers (->> (from-page full-market-url)
                           (merge {"Accept" "text/event-stream"}))
              force-coll #(if (coll? %) % [])
              value-fns {:timestamp #(java.time.Instant/ofEpochMilli (* 1000 (Float/parseFloat %)))
                         :noOrders force-coll
                         :yesOrders force-coll}
              resp (http-get url {:headers headers :as :stream})]
            (loop [should-repeat (try
                                    (each-line (:body resp) on-update value-fns #(continue-monitoring market-id contract-id))
                                    (catch Exception e
                                        (l/log :warn "Exception in monitor-order-book" (l/ex-log-msg e))
                                        (when (:should-retry (ex-data e)) (throw e))
                                        :recur))]
                (if (and (continue-monitoring market-id contract-id) (= :recur should-repeat))
                    (recur (http-get url {:headers headers :as :stream}))
                    nil)))))

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
    [auth]
    (l/with-log :debug "Get positions"
        (let [headers (->> (from-page (predictit-site-url "/markets"))
                        (merge (json-req))
                        (merge (with-auth auth)))
            value-fns {:userInvestment utils/to-decimal
                       :userMaxPayout utils/to-decimal
                       :userAveragePricePerShare utils/to-decimal}
            qs (str "?sort=traded&sortParameter=ALL")
            url (predictit-api-url (str "/Profile/Shares" qs))
            resp (http-get url {:headers headers})]
            (when-not (-> resp :body some?)
                (throw (ex-info "Bad portfolio response" {:resp resp})))
            (let [{suspended? :isTradingSuspended
                suspension-msg :isTradingSuspendedMessage
                mkts :markets} (resp-from-json resp value-fns)]
                (when suspended?
                    (throw (ex-info "TRADING SUSPENDED" {:suspension-msg suspension-msg})))
                {:positions mkts}))))

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
    [auth market-id full-market-url]
    (l/with-log :debug "Getting contracts"
        (let [headers (->> (from-page full-market-url)
                           (merge (json-req))
                           (merge (with-auth auth)))
              resp (http-get (predictit-api-url (str "/Market/" market-id "/Contracts")) {:headers headers})
              value-fns {:bestYesPrice utils/to-decimal
                         :bestNoPrice utils/to-decimal
                         :lastTradePrice utils/to-decimal
                         :lastClosePrice utils/to-decimal
                         :userAveragePricePerShare utils/to-decimal
                         :dateOpened utils/parse-isoish-datetime}]
            (when-not (-> resp :body some?)
                (throw (ex-info "Bad contracts response" {:resp resp})))
            {:contracts (resp-from-json resp)})))