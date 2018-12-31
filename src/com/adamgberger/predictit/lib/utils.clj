(ns com.adamgberger.predictit.lib.utils
  (:require [clojure.data.json :as json])
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
    (java.math.BigDecimal. n))

(defn parse-ext-iso-date
  "E.x. 1986-11-22"
  [s]
  (-> s
      java.time.LocalDate/parse
      (.atStartOfDay (java.time.ZoneId/of "UTC"))
      (.truncatedTo java.time.temporal.ChronoUnit/DAYS)
      (.toInstant)))

(defn parse-rfc1123-datetime
  "E.x. Fri, 28 Dec 2018 00:00:00 -0600"
  [s]
  (-> s
      (java.time.ZonedDateTime/parse (java.time.format.DateTimeFormatter/RFC_1123_DATE_TIME))
      (.toInstant)))

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