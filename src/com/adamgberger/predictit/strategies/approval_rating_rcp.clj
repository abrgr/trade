(ns com.adamgberger.predictit.strategies.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.multi-contract-portfolio-utils :as port-opt-utils]
            [com.adamgberger.predictit.lib.execution-utils :as execution-utils])
  (:gen-class))

(def pricing-math-ctx (java.math.MathContext. 2 java.math.RoundingMode/HALF_UP))

(defn is-relevant-mkt [mkt]
  (let [^String n (-> mkt
                      ^String (:market-name)
                      .toLowerCase)
        is-open (= (:status mkt) :open)
        is-trump (.contains n "trump")
        is-rcp (.contains n " rcp ")
        is-approval (.contains n " approval ")]
    (and is-open is-trump is-rcp is-approval)))

(defn adapt-contract-bounds [c-name]
    ; Match "45.3% or lower", "50.2% or higher", or "47.1% - 47.4%"
    ; Return a bound {:lower-inclusive x :upper-inclusive y} or nil
  (let [lower-match (re-matches #"(\d+[.]\d)% or lower" c-name)
        higher-match (re-matches #"(\d+[.]\d)% or higher" c-name)
        range-match (re-matches #"(\d+[.]\d)%\s*[-]\s*(\d+[.]\d)%" c-name)
        bounds (cond
                 (some? lower-match) {:lower-inclusive (utils/to-decimal -1100)
                                      :upper-inclusive (-> lower-match second utils/to-decimal)}
                 (some? higher-match) {:lower-inclusive (-> higher-match second utils/to-decimal)
                                       :upper-inclusive (utils/to-decimal 1000)}
                 (some? range-match) {:lower-inclusive (-> range-match second utils/to-decimal)
                                      :upper-inclusive (-> range-match next second utils/to-decimal)}
                 :else nil)]
    {:lower-inclusive (-> bounds :lower-inclusive (- 0.05M))
     :upper-inclusive (-> bounds :upper-inclusive (+ 0.05M))}))

(defn adapt-market-name-to-end-date [m-name]
    ; Deal with names like
    ; "What will Trump's RCP average job approval be at end of day Jan. 4?"
    ; or
    ; "Trump RCP avg. approval 1/4?"
  (let [num-match (re-matches #"^.*(\d{1,2})/(\d{1,2})[?]$" m-name)
        str-match (re-matches #"^.*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.]?\s*(\d+)[?]$" m-name)
        months {"Jan" 1 "Feb" 2 "Mar" 3 "Apr" 4 "May" 5 "Jun" 6 "Jul" 7 "Aug" 8 "Sep" 9 "Oct" 10 "Nov" 11 "Dec" 12}
        next-md (fn [^Integer m ^Integer d]
                  (let [today (java.time.ZonedDateTime/now utils/ny-time)
                        year (.getYear today)
                          ; TODO: get 23:59 from market
                        md-this-year (java.time.ZonedDateTime/of year m d 23 59 59 999 utils/ny-time)]
                    (if (>= (compare md-this-year today) 0)
                      md-this-year
                      (.withYear md-this-year (inc year)))))]
    (cond
      (some? num-match) (next-md (-> num-match second Integer/parseInt) (-> num-match next second Integer/parseInt))
      (some? str-match) (next-md (-> str-match second months) (-> str-match next second Integer/parseInt))
      :else nil)))

(defn adapt-contract [c]
  (let [bounds (-> c :contract-name adapt-contract-bounds)
        id (:contract-id c)]
    (if (or (nil? bounds))
      (do (l/log :error "Invalid RCP contract" {:contract c})
          nil)
      {:contract-id id
       :bounds bounds})))

(defn adapt-market [mkt contracts]
  (let [end-date (-> mkt :market-name adapt-market-name-to-end-date)
        invalid? (or (some nil? contracts)
                     (nil? end-date)
                     (< (compare end-date (java.time.ZonedDateTime/now utils/ny-time)) 0))]
    (if invalid?
      (do (l/log :error "Invalid market" {:mkt mkt
                                          :contracts contracts})
          nil)
      {:market-id (:market-id mkt)
       :market-url (:market-url mkt)
       :end-date end-date
       :contracts contracts})))

(defn adapt-mkt [contracts {:keys [market-id] :as mkt}]
   (->> (get contracts market-id)
        vals
        (mapv adapt-contract)
        (adapt-market mkt)))

(defn contract-prob-dist [^org.apache.commons.math3.distribution.AbstractRealDistribution dist contracts]
  (->> contracts
       (map
        (fn [contract]
          (let [bounds (:bounds contract)
                {:keys [lower-inclusive upper-inclusive]} bounds
                pr (some-> dist (.probability lower-inclusive upper-inclusive))]
            (if (some? pr)
              [(:contract-id contract) pr]
              nil))))
       (into {})))

(defn mkt-prob-dists [dist-by-days tradable-mkts]
  (->> tradable-mkts
       (map
        (fn [{:keys [market-id end-date contracts]}]
          (let [day-count (.between java.time.temporal.ChronoUnit/DAYS
                                    (java.time.ZonedDateTime/now utils/ny-time)
                                    end-date)
                dist (get dist-by-days day-count)]
            (if (some? dist)
              [market-id (contract-prob-dist dist contracts)]
              nil))))
       (filter some?)
       (into {})))

(defn calculate-prob-dists [rcp-estimate tradable-mkts]
  (let [{:keys [dists-by-day]} rcp-estimate]
    (when (and (some? tradable-mkts)
               (some? dists-by-day))
      (mkt-prob-dists dists-by-day tradable-mkts))))

(defn market-time-info [^java.time.ZonedDateTime end-date ^java.time.Instant latest-major-input-change]
  (let [mins-left (.between
                   java.time.temporal.ChronoUnit/MINUTES
                   (java.time.ZonedDateTime/now)
                   end-date)
        mins-since-latest-major-input-change (if (nil? latest-major-input-change)
                                               300
                                               (.between
                                                java.time.temporal.ChronoUnit/MINUTES
                                                latest-major-input-change
                                                (java.time.Instant/now)))
        fill-mins (cond
                    (< mins-left 180) 10.0
                    (< mins-since-latest-major-input-change 10) 1.0
                    :else 120.0)]
    {:end-date end-date
     :mins-left mins-left
     :mins-since-latest-major-input-change mins-since-latest-major-input-change
     :fill-mins fill-mins}))

(defn get-contract [mkt contract-id]
  (->> mkt
       :contracts
       (filter #(= (:contract-id %) contract-id))
       first))

(defn trades-for-mkt [hurdle-rate mkt orders-by-contract-id latest-major-input-change cur order-books-by-contract-id prob-by-contract-id prev]
  (let [{:keys [fill-mins]} (market-time-info (:end-date mkt) latest-major-input-change)
        contract-ids-with-order-books (->> order-books-by-contract-id
                                           keys
                                           (into #{}))
        contract-ids (->> prob-by-contract-id
                          keys
                          (into #{}))
        valid? (clojure.set/superset? contract-ids-with-order-books contract-ids)
        price-and-prob-for-contract (fn [[contract-id prob]]
                                      (let [contract (get-contract mkt contract-id)
                                            order-book (get order-books-by-contract-id contract-id)
                                            orders (get-in orders-by-contract-id [contract-id :orders])
                                            likely-fill-yes (execution-utils/get-likely-fill fill-mins prob order-book orders pricing-math-ctx :buy-yes)
                                            likely-fill-no (execution-utils/get-likely-fill fill-mins prob order-book orders pricing-math-ctx :buy-no)]
                                        (when (every? some? [likely-fill-yes likely-fill-no])
                                          {:prob-yes prob
                                           :price-yes (:price likely-fill-yes)
                                           :est-value-yes (:est-value likely-fill-yes)
                                           :price-no (:price likely-fill-no)
                                           :est-value-no (:est-value likely-fill-no)
                                           :fill-mins fill-mins
                                           :contract-id contract-id})))
        contracts-price-and-prob  (->> prob-by-contract-id
                                       (map price-and-prob-for-contract)
                                       (filter some?)
                                       (into []))
        prev' (->> prev
                   (map (fn [c] (assoc c :weight (:orig-weight c))))
                   (into {}))]
    (if valid?
      (->> (port-opt-utils/get-optimal-bets hurdle-rate contracts-price-and-prob prev)
           (mapv
             (fn [{:keys [weight] :as m}]
               ; Divide by 2 to accomodate 2 simultaneous markets on mondays & tuesdays
               (merge m {:orig-weight weight :weight (/ weight 2)}))))
      [])))

(defn calculate-trades [hurdle-rate
                        tradable-mkts
                        prob-dist
                        latest-major-input-change
                        rcp-est
                        order-books
                        orders
                        prev]
  (let [cur (:val rcp-est)
        get-mkt (fn [mkt-id]
                  (->> tradable-mkts
                       (filter #(= (:market-id %) mkt-id))
                       first))
        get-trades-for-market (fn [[mkt-id p-by-ctrct]]
                                  {mkt-id (trades-for-mkt
                                           hurdle-rate
                                           (get-mkt mkt-id)
                                           (get orders mkt-id)
                                           latest-major-input-change
                                           cur
                                           (get order-books mkt-id)
                                           p-by-ctrct
                                           (get prev mkt-id))})]
    (->> prob-dist
         (map get-trades-for-market)
         (apply merge {}))))

(defn- round-rcp [a]
  (.setScale a 1 java.math.RoundingMode/HALF_UP))

(defn calculate-major-input-change [est]
  (let [{:keys [est-at val]} est
        {prev-est-at :est-at
         prev-val :val} (-> est meta :com.adamgberger.predictit.lib.observable-state/prev)]
    (if (and (some? est-at)
             (some? prev-est-at)
             (not= (round-rcp val) (round-rcp prev-val)))
      (java.time.Instant/now)
      nil)))
