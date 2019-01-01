(ns com.adamgberger.predictit.inputs.approval-rating-538
  (:require [com.adamgberger.predictit.apis.json-url :as j]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(def id ::id)

(defn get-raw []
    (let [browser-url "https://projects.fivethirtyeight.com/trump-approval-ratings/"
          json-url "https://projects.fivethirtyeight.com/trump-approval-ratings/approval.json"
          value-fns {:date utils/parse-ext-iso-date
                     :approve_estimate utils/to-decimal
                     :disapprove_estimate utils/to-decimal}]
        (j/get-json browser-url json-url value-fns)))

(def subgroup-to-audience
    {"All polls" :all
     "Voters" :voters
     "Adults" :adults})

(def audience-to-subgroup (utils/rev-assoc subgroup-to-audience))

(defn to-canonical [item]
    {:date (:date item)
     :approval (:approve_estimate item)
     :disapproval (:disapprove_estimate item)
     :audience (-> item :subgroup subgroup-to-audience)})

(defn get-current
    "Returns something like:
     {:current {:date #inst 2018-12-31
                :approval 42.3
                :disapproval 57.7
                :audience :all}
      :retrieved-at #inst 2018-12-31T10:51:01.000Z}"
    ([current-date]
        (l/with-log :debug "Retrieving 538 trump approval ratings"
            (let [raw (get-raw)
                  retrieved-at (java.time.Instant/now)
                  raw-all (filter #(= (:subgroup %) (:all audience-to-subgroup)) raw)
                  tgt (-> current-date (.truncatedTo java.time.temporal.ChronoUnit/DAYS))
                  hist (filter (comp not :future) raw-all)
                  val (utils/glb-key hist tgt :date)]
                {:current (some-> val to-canonical)
                 :retrieved-at retrieved-at}))))