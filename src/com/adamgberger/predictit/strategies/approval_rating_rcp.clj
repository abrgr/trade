(ns com.adamgberger.predictit.strategies.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(def venue-id :com.adamgberger.predictit.venues.predictit/predictit)
(def rcp-input-id :com.adamgberger.predictit.inputs.approval-rating-rcp/id)

(def math-ctx (java.math.MathContext. 1 java.math.RoundingMode/HALF_UP))

(def confidence (utils/to-decimal "0.8"))

(defn is-relevant-mkt [mkt]
    (let [n (-> mkt :market-name .toLowerCase)
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
          next-md (fn [m d]
                    (let [today (java.time.LocalDate/now)
                          year (.getYear today)
                          md-this-year (java.time.LocalDate/of year m d)]
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
                       (< (compare end-date (java.time.LocalDate/now)) 0))]
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

(defn contract-prob-dist [dist-by-days end-date approval-rating contracts]
    (->> contracts
         (map
            (fn [contract]
                (let [day-count (.between java.time.temporal.ChronoUnit/DAYS
                                          (java.time.LocalDate/now)
                                          end-date)
                      dist (get dist-by-days day-count)
                      bounds (:bounds contract)
                      {:keys [lower-inclusive upper-inclusive]} bounds
                      l (- lower-inclusive approval-rating)
                      u (- upper-inclusive approval-rating)
                      pr (some-> dist (.probability l u))]
                    (if (some? pr)
                        [(:contract-id contract) pr]
                        nil))))
         (into {})))

(defn mkt-prob-dists [dist-by-days tradable-mkts approval-rating]
    (->> tradable-mkts
         (map (fn [mkt] [(:market-id mkt) (contract-prob-dist dist-by-days (:end-date mkt) approval-rating (:contracts mkt))]))
         (into {})))

(defn update-probs [state strat-state end-chan]
    (l/log :info "Updating RCP probabilities")
    (let [approval (-> @(:inputs state) rcp-input-id :current :approval)
          tradable-mkts (:tradable-mkts @strat-state)
          dist-by-days (:dist-by-days @strat-state)
          prob-dist (when (and (some? tradable-mkts)
                               (some? approval))
                          (mkt-prob-dists dist-by-days tradable-mkts approval))]
        (when (some? prob-dist)
            (send strat-state #(assoc % :prob-dist prob-dist)))))

(defn trade-by-prob [end-date ctrct cur latest-major-input-change p]
    (let [end-time (-> end-date
                       (.atTime 12 0) ; TODO: get this from the market
                       (.atZone (java.time.ZoneId/of "America/New_York")))
          mins-left (.between
                        java.time.temporal.ChronoUnit/MINUTES
                        (java.time.ZonedDateTime/now)
                        end-time)
          mins-since-latest-major-input-change (.between
                                                java.time.temporal.ChronoUnit/MINUTES
                                                latest-major-input-change
                                                (java.time.Instant/now))
          cur-winner? (and (>= cur (get-in ctrct [:bounds :lower-inclusive]))
                           (<= cur (get-in ctrct [:bounds :upper-inclusive])))
          adj-p  (cond
                    (and (< mins-left 180) (not cur-winner?)) 0
                    (and (< mins-left 180) cur-winner?) 1
                    ; TODO: add some decay towards the end
                    :else p)]
        {:type :target-prob
         :target adj-p
         :confidence confidence
         :max-stake-pct (utils/to-decimal "0.5")
         :urgency (cond
                    (< mins-left 180) :immediate
                    (< mins-since-latest-major-input-change 10) :immediate
                    :else :normal)}))

(defn update-trades [state strat-state end-chan]
    (let [ss @strat-state
          prob-dist (:prob-dist ss)
          mkts (:tradable-mkts ss)
          latest-major-input-change (:latest-major-input-change ss)
          cur (get-in @(get-in state [:inputs rcp-input-id]) [:current :approval])
          order-books (get-in @(:venue-state state) [venue-id :order-books])
          get-ctrct (fn [mkt ctrct-id]
                        (->> (:contracts mkt)
                             (filter #(= (:contract-id %) ctrct-id)
                             first)))
          trades-for-ctrct (fn [mkt [ctrct-id p]]
                            [ctrct-id (trade-by-prob (:end-date mkt) (get-ctrct mkt ctrct-id) cur latest-major-input-change p)])
          get-mkt (fn [mkt-id]
                    (->> mkts
                         (filter #(= (:market-id %) mkt-id))
                         first))
          trades-for-mkt (fn [[mkt-id p-by-ctrct]]
                            [mkt-id (trades-for-ctrct (get-mkt mkt-id) p-by-ctrct)])
          trades (->> prob-dist
                      (map trades-for-mkt)
                      (into {}))]
        (send (:venue-state state) #(assoc-in % [venue-id :req-pos ::id] trades))))

(defn maintain-trades [state strat-state end-chan]
    ; TODO: also tick update-trades more frequently as we near the end of the market
    (letfn [(upd [_]
                (update-trades state strat-state end-chan))
            (valid? [old new]
                (and (some? new)
                     (not= old new)))]
        (utils/add-guarded-watch-in
            strat-state
            ::maintain-trades
            [:prob-dist]
            valid?
            upd)
        (utils/add-guarded-watch-in
            strat-state
            ::maintain-trades
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
            (:inputs state)
            ::maintain-probs
            [rcp-input-id :current :approval]
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
            upd)
        (utils/add-guarded-watch-in
            strat-state
            ::maintain-probs-for-cfg
            [:dist-by-days]
            valid?
            upd)))

(defn maintain-major-input-changes [state strat-state end-chan]
    ; TODO: make a decent macro for this
    (letfn [(upd [_]
                (send strat-state #(assoc % :latest-major-input-change (java.time.Instant/now))))
            (valid? [old new]
                (and (some? new)
                     (and (some? old)
                           (not= (.round old math-ctx) (.round new math-ctx)))))]
        (utils/add-guarded-watch-in
            (:inputs state)
            ::maintain-probs
            [rcp-input-id :current :approval]
            valid?
            upd)))

(defn run [cfg state end-chan]
    (let [dist-by-days (->> cfg
                            :com.adamgberger.predictit.strategies.approval-rating-rcp
                            :dist-by-days
                            (map (fn [[days dist]]
                                [days (org.apache.commons.math3.distribution.NormalDistribution. (:mean dist) (:std dist))]))
                            (into {}))
          strat-state (l/logging-agent "rcp-strat" (agent {:dist-by-days dist-by-days}))]
          (maintain-relevant-mkts state strat-state end-chan)
          (maintain-order-books state strat-state end-chan)
          (maintain-local-markets state strat-state end-chan)
          (maintain-probs state strat-state end-chan)
          (maintain-major-input-changes state strat-state end-chan)
          {::state strat-state}))