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

(def cm (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10 :threads 10}))

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
    ([resp] (resp-from-json {}))
    ([resp value-fns]
        (-> resp
           :body
           (utils/read-json value-fns))))

(defn parse-localish-datetime [s]
    (if (= s "N/A")
        nil
        (try
            (let [formatter (java.time.format.DateTimeFormatter/ofPattern "MM/dd/yyyy hh:mm a (z)")
                zoned (java.time.ZonedDateTime/parse s formatter)]
                (.toInstant zoned))
            (catch java.time.format.DateTimeParseException e
                (l/log :warn "Un-parsable localish datetime" {:input-string s})
                nil))))

(defn parse-offset-datetime [s]
    (try
        (-> s
            (java.time.ZonedDateTime/parse (java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))
            .toInstant)
        (catch java.time.format.DateTimeParseException e
            (l/log :warn "Un-parsable offset datetime" {:input-string s}))))

(defn parse-isoish-datetime [s]
    (try
        (java.time.Instant/parse (if (.endsWith s "Z") s (str s "Z")))
        (catch java.time.format.DateTimeParseException e
            (l/log :warn "Un-parsable isoish datetime" {:input-string s}))))

(defn each-line [is f value-fns]
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
                (recur (.readLine rdr) (.readLine rdr))))))

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
            value-fns {:.issued parse-offset-datetime
                       :.expires parse-offset-datetime}]
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
            resp (http-get (predictit-api-url "/User/Wallet/Balance") {:headers headers})]
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
         :status \"Open\"
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
              value-fns {:endDate parse-localish-datetime
                         :startDate parse-isoish-datetime}]
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
     Invokes on-update with each order book update.  Updates look like:
     {:noOrders []
      :timestamp #inst 1546213580.64377
      :yesOrders [
        {:costPerShareNo 0.99
         :costPerShareYes 0.01
         :pricePerShare 0.01
         :quantity 4453
         :tradeType 0}
        ...]}"
    [auth market-id market-name contract-id on-update]
    (l/with-log :debug "Monitor order-book"
        (let [url (predictit-firebase-url (str "/contractOrderBook/" contract-id ".json"))
              headers (->> (from-page (predictit-site-url (str "/markets/detail/" market-id "/" (.replace market-name " " "-"))))
                           (merge {"Accept" "text/event-stream"}))
              force-coll #(if (coll? %) % [])
              value-fns {:timestamp #(java.time.Instant/ofEpochMilli (* 1000 (Float/parseFloat %)))
                         :noOrders force-coll
                         :yesOrders force-coll}
              resp (http-get url {:headers headers :as :stream})]
            (loop [should-repeat (try
                                    (each-line (:body resp) on-update value-fns)
                                    (catch Exception e
                                        (l/log :warn "Exception in monitor-order-book" (l/ex-log-msg e))
                                        (when (:should-retry (ex-data e)) (throw e))
                                        :recur))]
                (if (= :recur should-repeat)
                    (recur (http-get url {:headers headers :as :stream}))
                    nil)))))