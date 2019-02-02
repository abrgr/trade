(ns com.adamgberger.predictit.estimators.rasmussen
  (:require [hickory.core :as m]
            [hickory.select :as s]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as rcp-in]))

(defn- get-child [h n]
    (->> h
         (s/select (s/child (s/tag :tr) (s/nth-child n)))
         next
         (map (comp first :content first :content))))

(defn get-ras []
    (let [h (-> (slurp "rcp/rasmussen_hist.html")
                m/parse
                m/as-hickory)
          date-pat (java.time.format.DateTimeFormatter/ofPattern "dd-MMM-yy")
          dates (->> (get-child h 1)
                     (map #(java.time.LocalDate/parse % date-pat)))
          vals (->> (get-child h 5)
                    (map #(.substring % 0 (dec (count %))))
                    (map #(BigDecimal. %)))
          data (map
                (fn [date val] {:date date
                                :val val})
                dates
                vals)]
        data))

(defn get-rcp []
    (let [data (-> "rcp/6179_historical.json"
                    slurp
                    (utils/read-json {:value utils/to-decimal})
                    :poll
                    :rcp_avg
                    '#rcp-in/to-canonical)]
        data))

(defn get-data []
    (let [ras (get-ras)
          rcp (get-rcp)]
        {:ras ras
         :rcp rcp}))