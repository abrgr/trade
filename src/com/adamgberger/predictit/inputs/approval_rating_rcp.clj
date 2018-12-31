(ns com.adamgberger.predictit.inputs.approval-rating-rcp
  (:require [com.adamgberger.predictit.apis.json-url :as j]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(defn remove_js
    "They return something like return_json({...the stuff we want})"
    [s]
    (as-> s s
          (.substring s (clojure.string/index-of s "{"))
          (.substring s 0 (inc (clojure.string/last-index-of s "}")))))

(defn get-raw []
    (let [browser-url "https://www.realclearpolitics.com/epolls/other/president_trump_job_approval-6179.html"
          json-url "https://www.realclearpolitics.com/epolls/json/6179_historical.js?1546273265398&callback=return_json"
          value-fns {:date (comp utils/truncated-to-day utils/parse-rfc1123-datetime)
                     :value utils/to-decimal}]
        (j/get-json browser-url json-url value-fns remove_js)))

(defn to-canonical [item]
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

(defn get-current
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