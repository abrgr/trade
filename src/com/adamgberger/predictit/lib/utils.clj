(ns com.adamgberger.predictit.lib.utils
  (:require [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(def price-mc (java.math.MathContext. 2 java.math.RoundingMode/HALF_UP))

(declare merge-deep)

(defn deep-merger [v1 v2]
  (if (and (map? v2) (map? v2))
    (merge-deep v1 v2)
    v2))

(defn merge-deep [& maps]
  (apply merge-with deep-merger maps))

(defn split-first [^String haystack ^String needle]
  (let [idx (.indexOf haystack needle)]
    (if (neg? idx)
      [haystack ""]
      [(.substring haystack 0 idx) (.substring haystack (inc idx))])))

(defn read-json [str value-fns]
    (json/read-str str
                   :key-fn keyword
                   :value-fn (fn [k v]
                                (let [f (k value-fns)]
                                    (if (some? f)
                                        (f v)
                                        v)))))

(defn to-decimal [n]
    (if (nil? n)
      nil
      (cond
        (instance? String n) (java.math.BigDecimal. ^String n)
        (instance? Double n) (java.math.BigDecimal. ^Double n)
        :else (java.math.BigDecimal. n))))

(defn to-price [n]
  (-> n
      to-decimal
      (.round price-mc)))

(defn parse-ext-iso-date
  "E.x. 1986-11-22"
  [s]
  (some-> s
      java.time.LocalDate/parse
      (.atStartOfDay (java.time.ZoneId/of "UTC"))
      (.truncatedTo java.time.temporal.ChronoUnit/DAYS)
      (.toInstant)))

(defn parse-rfc1123-datetime
  "E.x. Fri, 28 Dec 2018 00:00:00 -0600"
  [s]
  (some-> s
      (java.time.ZonedDateTime/parse (java.time.format.DateTimeFormatter/RFC_1123_DATE_TIME))
      (.toInstant)))

(defn parse-localish-datetime
  "E.x. 11/22/1986 12:30 AM (ET)"
  [s]
  (if (or (nil? s) (= s "N/A"))
      nil
      (try
          (let [formatter (java.time.format.DateTimeFormatter/ofPattern "MM/dd/yyyy hh:mm a (z)")
              zoned (java.time.ZonedDateTime/parse s formatter)]
              (.toInstant zoned))
          (catch java.time.format.DateTimeParseException e
              (l/log :warn "Un-parsable localish datetime" {:input-string s})
              nil))))

(defn parse-offset-datetime
  "E.x. 2011-12-03T10:15:30+01:00"
  [s]
  (try
      (some-> s
          (java.time.ZonedDateTime/parse (java.time.format.DateTimeFormatter/ISO_OFFSET_DATE_TIME))
          .toInstant)
      (catch java.time.format.DateTimeParseException e
          (l/log :warn "Un-parsable offset datetime" {:input-string s}))))

(defn parse-isoish-datetime
  "E.x. 2018-12-03T11:30:20.00"
  [^String s]
  (try
    (if (nil? s)
      nil
      (java.time.Instant/parse (if (.endsWith s "Z") s (str s "Z"))))
    (catch java.time.format.DateTimeParseException e
        (l/log :warn "Un-parsable isoish datetime" {:input-string s}))))

(defn parse-human-date-with-day
  "E.x. Thursday, January 10, 2019"
  [s]
  (try
    (java.time.LocalDate/parse s (java.time.format.DateTimeFormatter/ofPattern "EEEE, MMMM dd, yyyy"))
    (catch java.time.format.DateTimeParseException e
      (l/log :warn "Un-parsable human date with day" {:input-string s}))))

(defn parse-human-date-range-within
  "E.x. January 10 - 12, 2019"
  [s]
  (let [date-regex #"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d+)\s*[-]\s*(\d+)\s*,\s*(\d+)"
        match (re-find date-regex s)
        [_ month start-day end-day year] match]
    (if (nil? match)
      (do (l/log :warn "Cannot find human date range" {:input-string s})
          nil)
      {:start-date (java.time.LocalDate/parse
                    (str month " " start-day " " year)
                    (java.time.format.DateTimeFormatter/ofPattern "MMMM d y"))
       :end-date (java.time.LocalDate/parse
                  (str month " " end-day " " year)
                  (java.time.format.DateTimeFormatter/ofPattern "MMMM d y"))})))

(defn parse-human-date-within
  "E.x. January 10, 2019"
  [s]
  (let [date-regex #"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d+)\s*,\s*(\d+)"
        match (re-find date-regex s)
        [_ month day year] match]
    (if (nil? match)
      (do (l/log :warn "Cannot find human date" {:input-string s})
          nil)
      (java.time.LocalDate/parse
        (str month " " day " " year)
        (java.time.format.DateTimeFormatter/ofPattern "MMMM d y")))))

(defn most-recent-month-day
  [^Integer month ^Integer day]
  (let [today (java.time.LocalDate/now)
        year (-> today
                 (.get (java.time.temporal.ChronoField/YEAR)))
        md-this-year (java.time.LocalDate/of year month day)]
    (if (> (compare md-this-year today) 0)
      (java.time.LocalDate/of (dec year) month day)
      md-this-year)))

(defn parse-historical-month-day-range
  "E.x. 1/10 - 1/15"
  [s]
  (let [[from to] (string/split s #"\s*[-]\s*")
        [from-mon from-day] (map #(Integer/parseInt %) (string/split from #"/"))
        [to-mon to-day] (map #(Integer/parseInt %) (string/split to #"/"))]
    {:from (most-recent-month-day from-mon from-day)
     :to (most-recent-month-day to-mon to-day)}))

(defn truncated-to-day
  [d]
  (.truncatedTo d java.time.temporal.ChronoUnit/DAYS))

(def weekend-days? #{6 7})

(def ny-time (java.time.ZoneId/of "America/New_York"))

(defn next-weekday-at [^java.time.LocalDate local-date ^java.time.ZoneId tz ^Integer hour ^Integer minute]
  (let [tomorrow (-> local-date
                     (.atStartOfDay tz)
                     (.plus 1 java.time.temporal.ChronoUnit/DAYS))
        tomorrow-day (.get tomorrow java.time.temporal.ChronoField/DAY_OF_WEEK)]
      (if (weekend-days? tomorrow-day)
          (next-weekday-at (.toLocalDate tomorrow) tz hour minute)
          (-> tomorrow
              (.withHour hour)
              (.withMinute minute)))))

(defn glb-key
  "Returns the greatest item in items such that (<= (key-fn item) tgt)"
  [items tgt key-fn]
  (let [potential-glb (reduce
                        (fn 
                          ([] nil)
                          ([glb next]
                            (let [glb-key (key-fn glb)
                                  next-key (key-fn next)
                                  next-ok (and (some? next-key) (<= (compare next-key tgt) 0))
                                  c (compare glb-key next-key)]
                              (if (or (not next-ok) (>= c 0))
                                  glb
                                  next))))
                        items)]
    (if (<= (compare (key-fn potential-glb) tgt) 0)
      potential-glb
      nil)))

(defn rev-assoc
  "Reverse the map.  Given {:a :b}, return {:b :a}"
  [m]
  (->> m
      (map (fn [[k v]] [v k]))
      (into {})))

(defn repeatedly-until-closed [f interval-ms done end-chan]
  (async/go-loop []
    (f)
    (let [interval (if (number? interval-ms) interval-ms (interval-ms))
          jittered-interval (Math/floor (+ interval (* 0.2 interval (rand))))
          keep-going? (async/alt!
                        end-chan false
                        (async/timeout jittered-interval) true)]
      (if keep-going?
        (recur)
        (done)))))

(defn add-guarded-watch-in [agt id keypath guard f]
    (letfn [(w [_ _ old new]
                (let [new-v (get-in new keypath)
                      old-v (get-in old keypath)]
                    (when (guard old-v new-v) (f new-v))))]
        (add-watch agt id w)))

(defn add-guarded-watch-ins [agt id keypaths guard f]
    (letfn [(w [_ _ old new]
                (let [new-vs (map (partial get-in new) keypaths)
                      old-vs (map (partial get-in old) keypaths)]
                    (when (guard old-vs new-vs) (f new-vs))))]
        (add-watch agt id w)))