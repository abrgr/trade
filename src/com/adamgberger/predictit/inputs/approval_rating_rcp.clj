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

(def hist ::hist)

(defn remove_js
  "They return something like return_json({...the stuff we want})"
  [^String s]
  (as-> s s
    (.substring s (string/index-of s "{"))
    (.substring s 0 (inc (string/last-index-of s "}")))))

(defn- get-raw [cb cb-err]
  (let [browser-url "https://www.realclearpolitics.com/epolls/other/president_trump_job_approval-6179.html"
        json-url "https://www.realclearpolitics.com/epolls/json/6179_historical.js?1546273265398&callback=return_json"
        value-fns {:date (comp utils/truncated-to-day utils/parse-rfc1123-datetime)
                   :value utils/to-decimal}]
    (j/async-get-json browser-url json-url value-fns remove_js cb cb-err)))

(defn to-canonical [{candidates :candidate, date :date}]
  (let [approve (->> candidates
                     (filter #(= (:name %) "Approve"))
                     first)
        disapprove (->> candidates
                        (filter #(= (:name %) "Disapprove"))
                        first)]
    {:date date
     :approval (:value approve)
     :disapproval (:value disapprove)
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
                                       (#(when (-> % :count pos?)
                                           (.divide ^java.math.BigDecimal (:sum %) ^java.math.BigDecimal (:count %) (java.math.MathContext. 6 java.math.RoundingMode/HALF_UP)))))]
    (if (nil? avg)
      nil
      {:rounded (.round avg (java.math.MathContext. 3 java.math.RoundingMode/HALF_UP))
       :exact avg})))

(defn- get-constituents [start-date end-date vals-by-src-date]
  (->> vals-by-src-date
       (map
        (fn [[src vals-by-date]]
          (let [[end val] (apply min-key (comp #(.until % (java.time.LocalDate/now) java.time.temporal.ChronoUnit/DAYS) first) vals-by-date)]
            [src (assoc val :end end)])))
       (filter
        (fn [[src val]]
          (and (>=  (compare (:start val) start-date) 0)
               (<= (compare (:end val) end-date) 0))))
       (filter #(and some?
                     (not= (first %) "RCP Average")
                     (not-empty (second %))))
       (into {})))

(defn- extract-current [html cb]
  (let [p (-> html m/parse m/as-hickory)
          ; Must use polling-data-rcp instead of polling-data-full because there is no rhyme or reason
          ; to what is included in the average.  The date filtering in get-constituents is bs.
        rows (s/select (s/child (s/descendant (s/id :polling-data-rcp)
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
                                   (assoc-in by-src-date [(string/replace poll #"\s+|\h+" " ") ; sometimes they use nbsp (ascii 160)
                                                          (:to date-range)] {:val val
                                                                             :start (:from date-range)})))
                               {}))
        rcp (get vals-by-src-date "RCP Average")
        avg (-> rcp vals first :val)
        start-date (-> rcp vals first :start)
        end-date (-> rcp keys first)
        constituents (get-constituents start-date end-date vals-by-src-date)
        recalculated-avg (recalculate-average constituents)
        valid-constituents? (and (some? recalculated-avg)
                                 (-> recalculated-avg
                                     :rounded
                                     (= avg)))]
    (when-not valid-constituents?
      (l/log :error "Invalid recalculated RCP average" {:recalculated-avg recalculated-avg
                                                        :avg avg
                                                        :start-date start-date
                                                        :end-date end-date
                                                        :constituents constituents
                                                        :vals-by-src-date vals-by-src-date}))
    (cb {:retrieved-at (java.time.Instant/now)
         :val avg
         :date end-date
         :start-date start-date
         :constituents constituents
         :valid? valid-constituents?
         :exact (if valid-constituents?
                  (:exact recalculated-avg)
                  avg)})))

(defn get-current [cb]
  (h/get
   "https://www.realclearpolitics.com/epolls/other/president_trump_job_approval-6179.html"
   {:async? true}
   #(extract-current (:body %) cb)
   #(l/log :error "Failed to get RCP approval" (l/ex-log-msg %))))

(defn get-hist
  "Returns something like:
     {:vals [{:date #inst 2018-12-31
              :approval 42.3
              :disapproval 57.7
              :audience :all} ...]
      :retrieved-at #inst 2018-12-31T10:51:01.000Z}"
  ([cb]
   (l/with-log :debug "Retrieving RCP trump approval ratings"
     (get-raw
      #(let [raw %
             retrieved-at (java.time.Instant/now)
             hist-vals (->> raw
                            :poll
                            :rcp_avg
                            (map to-canonical)
                            (into []))]
         (cb {:vals hist-vals
              :retrieved-at retrieved-at}))
      #(l/log :error "Failed to get historical RCP" (l/ex-log-msg %))))))
