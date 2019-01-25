(ns com.adamgberger.predictit.strategies.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.multi-contract-portfolio-utils :as port-opt-utils]
            [com.adamgberger.predictit.lib.execution-utils :as execution-utils])
  (:gen-class))

(def venue-id :com.adamgberger.predictit.venues.predictit/predictit)
(def rcp-estimate-id :com.adamgberger.predictit.estimators.approval-rating-rcp/id)

(def rcp-math-ctx (java.math.MathContext. 1 java.math.RoundingMode/HALF_UP))
(def pricing-math-ctx (java.math.MathContext. 2 java.math.RoundingMode/HALF_UP))

(def confidence (utils/to-decimal "0.8"))

(defn is-relevant-mkt [mkt]
    (let [^String n (-> mkt
                        ^String (:market-name)
                        .toLowerCase)
          is-open (= (:status mkt) :open)
          is-trump (.contains n "trump")
          is-rcp (.contains n " rcp ")
          is-approval (.contains n " approval ")]
        (and is-open is-trump is-rcp is-approval)))

(defn maintain-relevant-mkts [state strat-state end-chan]
  (l/log :info "Starting relevant market maintenance for RCP")
  (let [rel-chan (async/chan)]
    ; kick off a go routine to filter markets down to relevant markets
    ; and update the strategy with the results
    (async/go-loop []
      (let [[mkts _] (async/alts! [rel-chan end-chan])]
        (when (some? mkts)
          (let [rel-mkts (->> mkts
                              (filter is-relevant-mkt)
                              (into []))]
            (l/log :info "Updating relevant markets for RCP" {:rel-count (count rel-mkts)
                                                              :total-count (count mkts)})
            (send strat-state #(merge % {:mkts rel-mkts}))
            (recur)))))
    ; watch for changes to predictit markets and send new values to our go routine
    (utils/add-guarded-watch-in
        (:venue-state state)
        ::rel-mkts
        [venue-id :mkts]
        #(and (not= %1 %2) (some? %2))
        #(async/>!! rel-chan %))))

(defn maintain-order-books [state strat-state end-chan]
    (utils/add-guarded-watch-in
        strat-state
        ::maintain-order-books
        [:mkts]
        not=
        (fn [mkts]
            (let [needed-order-books (->> mkts
                                          (map #(select-keys % [:market-id :market-url :market-name]))
                                          (into #{}))
                  updater #(assoc-in % [venue-id :req-order-books ::id] needed-order-books)
                  venue-state (:venue-state state)]
                (l/log :info "Requesting order books" {:needed-order-books needed-order-books})
                (send venue-state updater)))))

(defn adapt-contract-bounds [c-name]
    ; Match "45.3% or lower", "50.2% or higher", or "47.1% - 47.4%"
    ; Return a bound {:lower-inclusive x :upper-inclusive y} or nil
    (let [lower-match (re-matches #"(\d+[.]\d)% or lower" c-name)
          higher-match (re-matches #"(\d+[.]\d)% or higher" c-name)
          range-match (re-matches #"(\d+[.]\d)%\s*[-]\s*(\d+[.]\d)%" c-name)]
        (cond
            (some? lower-match) {:lower-inclusive (utils/to-decimal -1100)
                                 :upper-inclusive (-> lower-match second utils/to-decimal)}
            (some? higher-match) {:lower-inclusive (-> higher-match second utils/to-decimal)
                                  :upper-inclusive (utils/to-decimal 1000)}
            (some? range-match) {:lower-inclusive (-> range-match second utils/to-decimal)
                                 :upper-inclusive (-> range-match next second utils/to-decimal)}
            :else nil)))

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
                            (.withYear md-this-year (inc year))))) ]
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

(defn maintain-local-markets [state strat-state end-chan]
    (utils/add-guarded-watch-in
        (:venue-state state)
        ::maintain-local-markets
        [venue-id :contracts]
        not=
        (fn [contracts-by-mkt-and-ctrct]
            (let [mkts (:mkts @strat-state) ; TODO: are we guaranteed that @strat-state is at least as fresh as venue-state?
                  adapt-mkt (fn [mkt]
                                (let [id (:market-id mkt)
                                      adapted-contracts (->> (get contracts-by-mkt-and-ctrct id)
                                                             vals
                                                             (map adapt-contract)
                                                             (into []))]
                                    (adapt-market mkt adapted-contracts)))
                  adapted-mkts (->> mkts
                                    (map adapt-mkt)
                                    (filter some?)
                                    (into []))
                  updater #(assoc % :tradable-mkts adapted-mkts)]
                (send strat-state updater)))))

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

(defn update-probs [state strat-state end-chan]
    (l/log :info "Updating RCP probabilities")
    (let [{:keys [dists-by-day]} (-> @(:estimates state) rcp-estimate-id)
          tradable-mkts (:tradable-mkts @strat-state)
          prob-dist (when (and (some? tradable-mkts)
                               (some? dists-by-day))
                        (mkt-prob-dists dists-by-day tradable-mkts))]
        (when (some? prob-dist)
            (send strat-state #(assoc % :prob-dist prob-dist)))))

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

(defn trades-for-mkt [hurdle-rate mkt latest-major-input-change cur order-books-by-contract-id prob-by-contract-id]
    (let [time-info (market-time-info (:end-date mkt) latest-major-input-change)
          {:keys [fill-mins]} time-info
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
                                              likely-fill (execution-utils/get-likely-fill fill-mins prob order-book pricing-math-ctx)]
                                            (when (some? likely-fill)
                                                {:prob prob
                                                 :est-value (:est-value likely-fill)
                                                 :fill-mins fill-mins
                                                 :trade-type (:trade-type likely-fill)
                                                 :price (:price likely-fill)
                                                 :contract-id contract-id})))
          contracts-price-and-prob  (->> prob-by-contract-id
                                         (map price-and-prob-for-contract)
                                         (filter some?)
                                         (into []))]
        (if valid?
            (port-opt-utils/get-optimal-bets hurdle-rate contracts-price-and-prob)
            [])))

(defn update-trades [state strat-state end-chan hurdle-rate]
    (l/with-log :info "Updating trades"
        (let [{mkts :tradable-mkts :keys [prob-dist latest-major-input-change]} @strat-state
              cur (get-in @(:estimates state) [rcp-estimate-id :val])
              order-books (get-in @(:venue-state state) [venue-id :order-books])
              get-mkt (fn [mkt-id]
                          (->> mkts
                              (filter #(= (:market-id %) mkt-id))
                              first))
              get-trades-for-market (fn [[mkt-id p-by-ctrct]]
                                        {mkt-id (trades-for-mkt
                                                    hurdle-rate
                                                    (get-mkt mkt-id)
                                                    latest-major-input-change
                                                    cur
                                                    (get order-books mkt-id)
                                                    p-by-ctrct)})
              trades (->> prob-dist
                          (map get-trades-for-market)
                          (apply merge {}))]
            (send (:venue-state state) #(assoc-in % [venue-id :req-pos ::id] trades)))))

(defn maintain-trades [state strat-state end-chan hurdle-rate]
    ; TODO: also tick update-trades more frequently as we near the end of the market
    (letfn [(upd [_]
                (update-trades state strat-state end-chan hurdle-rate))
            (valid? [old new]
                (and (some? new)
                     (not= old new)))]
        (utils/add-guarded-watch-in
            (:venue-state state)
            ::maintain-trades-order-books
            [venue-id :order-books]
            not=
            upd)
        (utils/add-guarded-watch-in
            (:venue-state state)
            ::maintain-trades-bal
            [venue-id :bal]
            not=
            upd)
        (utils/add-guarded-watch-in
            (:venue-state state)
            ::maintain-trades-pos
            [venue-id :pos]
            not=
            upd)
        (utils/add-guarded-watch-in
            strat-state
            ::maintain-trades-prob
            [:prob-dist]
            valid?
            upd)
        (utils/add-guarded-watch-in
            strat-state
            ::maintain-trades-major-changes
            [:latest-major-input-change]
            valid?
            upd)))

(defn maintain-probs [state strat-state end-chan]
    ; TODO: make a decent macro for this
    (letfn [(upd [_]
                (update-probs state strat-state end-chan))
            (valid? [old new]
                (and (some? new)
                     (not= old new)))]
        (utils/add-guarded-watch-in
            (:estimates state)
            ::maintain-probs
            [rcp-estimate-id :val]
            valid?
            upd)
        (utils/add-guarded-watch-in
            (:venue-state state)
            ::maintain-probs
            [venue-id :contracts]
            valid?
            upd)
        (utils/add-guarded-watch-in
            strat-state
            ::maintain-probs-for-mkts
            [:tradable-mkts]
            valid?
            upd)))

(defn maintain-major-input-changes [state strat-state end-chan]
    ; TODO: make a decent macro for this
    (letfn [(upd [_]
                (send strat-state #(assoc % :latest-major-input-change (java.time.Instant/now))))
            (valid? [^java.math.BigDecimal old ^java.math.BigDecimal new]
                (and (some? new)
                     (and (some? old)
                           (not= (.round old rcp-math-ctx) (.round new rcp-math-ctx)))))]
        (utils/add-guarded-watch-in
            (:estimates state)
            ::maintain-major-input-changes
            [rcp-estimate-id :val]
            valid?
            upd)))

(defn run [cfg state end-chan]
    (let [{:keys [hurdle-rate]} (::id cfg)
          strat-state (l/logging-agent "rcp-strat" (agent {}))]
        (when (nil? hurdle-rate)
            (throw (ex-info "Bad config for rcp strategy" {:cfg (::id cfg)})))
        (maintain-relevant-mkts state strat-state end-chan)
        (maintain-order-books state strat-state end-chan)
        (maintain-local-markets state strat-state end-chan)
        (maintain-probs state strat-state end-chan)
        (maintain-trades state strat-state end-chan hurdle-rate)
        (maintain-major-input-changes state strat-state end-chan)
        {::state strat-state}))