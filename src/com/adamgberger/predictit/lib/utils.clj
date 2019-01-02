(ns com.adamgberger.predictit.lib.utils
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(declare merge-deep)

(defn deep-merger [v1 v2]
  (if (and (map? v2) (map? v2))
    (merge-deep v1 v2)
    v2))

(defn merge-deep [& maps]
  (apply merge-with deep-merger maps))

(defn split-first [haystack needle]
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
      (java.math.BigDecimal. n)))

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
  [s]
  (try
    (if (nil? s)
      nil
      (java.time.Instant/parse (if (.endsWith s "Z") s (str s "Z"))))
    (catch java.time.format.DateTimeParseException e
        (l/log :warn "Un-parsable isoish datetime" {:input-string s}))))

(defn truncated-to-day
  [d]
  (.truncatedTo d java.time.temporal.ChronoUnit/DAYS))

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
          keep-going? (async/alt!
                        end-chan false
                        (async/timeout interval) true)]
      (if keep-going?
        (recur)
        (done)))))

(defn add-guarded-watch-in [agt id keypath guard f]
    (letfn [(w [_ _ old new]
                (let [new-v (get-in new keypath)
                      old-v (get-in old keypath)]
                    (when (guard old-v new-v) (f new-v))))]
        (add-watch agt id w)))