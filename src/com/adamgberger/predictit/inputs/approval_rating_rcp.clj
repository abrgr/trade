(ns com.adamgberger.predictit.inputs.approval-rating-rcp
  (:require [clojure.string :as string]
            [clj-http.client :as h]
            [hickory.core :as m]
            [hickory.select :as s]
            [com.adamgberger.predictit.apis.json-url :as j]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(def id ::id)

(defn remove_js
    "They return something like return_json({...the stuff we want})"
    [^String s]
    (as-> s s
          (.substring s (string/index-of s "{"))
          (.substring s 0 (inc (string/last-index-of s "}")))))

(defn- get-raw []
    (let [browser-url "https://www.realclearpolitics.com/epolls/other/president_trump_job_approval-6179.html"
          json-url "https://www.realclearpolitics.com/epolls/json/6179_historical.js?1546273265398&callback=return_json"
          value-fns {:date (comp utils/truncated-to-day utils/parse-rfc1123-datetime)
                     :value utils/to-decimal}]
        (j/get-json browser-url json-url value-fns remove_js)))

(defn- to-canonical [item]
    (let [candidates (:candidate item)
          approve (->> candidates
                       (filter #(= (:name %) "Approve"))
                       first)
          disapprove (->> candidates
                          (filter #(= (:name %) "Disapprove"))
                          first)]
        {:date (:date item)
         :approval (:value approve)
         :disapproval (:value approve)
         :audience :all}))

(defn- extract-content [el]
    (let [c (:content el)]
        (cond
            (string? el) el
            (some? c) (extract-content c)
            (coll? el) (->> el
                            (map extract-content)
                            (filter some?)
                            first)
            :else el)))

(defn recalculate-average [constituents]
    (let [^java.math.BigDecimal avg (->> constituents
                                         vals
                                         (reduce
                                             (fn [{:keys [count sum]} {:keys [val]}]
                                                     {:count (inc count)
                                                     :sum (+ sum val)})
                                             {:count java.math.BigDecimal/ZERO
                                              :sum java.math.BigDecimal/ZERO})
                                         (#(.divide ^java.math.BigDecimal (:sum %) ^java.math.BigDecimal (:count %) (java.math.MathContext. 6 java.math.RoundingMode/HALF_UP))))]
        {:rounded (.round avg (java.math.MathContext. 3 java.math.RoundingMode/HALF_UP))
         :exact avg}))

(defn- get-constituents [start-date end-date vals-by-src-date]
    (->> vals-by-src-date
        (map
            (fn [[src vals-by-date]]
                (let [[end val] (apply min-key (comp #(.until % (java.time.LocalDate/now) java.time.temporal.ChronoUnit/DAYS) first) vals-by-date)]
                    [src (assoc val :end end)])))
        (filter
            (fn [[src val]]
                (and (>  (compare (:start val) start-date) 0)
                     (<= (compare (:end val) end-date) 0))))
        (filter #(and some?
                      (not= (first %) "RCP Average")
                      (not-empty (second %))))
        (into {})))

(defn- extract-current [html cb]
    (let [p (-> html m/parse m/as-hickory)
          rows (s/select (s/child (s/descendant (s/id :polling-data-full)
                                                (s/and (s/class :data)
                                                       (s/tag :table))
                                                (s/tag :tr)))
                         p)
          header (-> rows
                     first
                     :content
                     (map #(-> % :content first)))
          vals-by-src-date (->> rows
                                next
                                (take 50) ; make sure we don't let years wrap around
                                (map #(map extract-content (:content %)))
                                (reduce
                                    (fn [by-src-date row]
                                        (let [poll (first row)
                                              date-range (utils/parse-historical-month-day-range (second row))
                                              val (utils/to-decimal (nth row 3))]
                                            (assoc-in by-src-date [poll (:to date-range)] {:val val
                                                                                           :start (:from date-range)})))
                                    {}))
          rcp (get vals-by-src-date "RCP Average")
          avg (-> rcp vals first :val)
          start-date (-> rcp vals first :start)
          end-date (-> rcp keys first)
          constituents (get-constituents start-date end-date vals-by-src-date)
          recalculated-avg (recalculate-average constituents)
          valid-constituents? (-> recalculated-avg
                                  :rounded
                                  (= avg))]
        (when-not valid-constituents?
            (l/log :error "Invalid recalculated RCP average" {:recalculated-avg recalculated-avg
                                                              :avg avg
                                                              :constituents constituents
                                                              :vals-by-src-date vals-by-src-date}))
        (cb {:retrieved-at (java.time.Instant/now)
             :val avg
             :date end-date
             :start-date start-date
             :constituents constituents
             :exact (:exact recalculated-avg)})))

(defn get-current [cb]
    (h/get
        "https://www.realclearpolitics.com/epolls/other/president_trump_job_approval-6179.html"
        {:async? true}
        #(extract-current (:body %) cb)
        #(l/log :error "Failed to get RCP approval" (l/ex-log-msg %))))

; TODO: this is just preserved here for now.  Used to be the way we got current before we needed constituents
(defn get-hist
    "Returns something like:
     {:current {:date #inst 2018-12-31
                :approval 42.3
                :disapproval 57.7
                :audience :all}
      :retrieved-at #inst 2018-12-31T10:51:01.000Z}"
    ([current-date]
        (l/with-log :debug "Retrieving RCP trump approval ratings"
            (let [raw (get-raw)
                  retrieved-at (java.time.Instant/now)
                  tgt (utils/truncated-to-day current-date)
                  val (utils/glb-key (-> raw :poll :rcp_avg) tgt :date)]
                {:current (some-> val to-canonical)
                 :retrieved-at retrieved-at}))))