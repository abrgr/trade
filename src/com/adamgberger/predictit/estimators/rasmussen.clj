(ns com.adamgberger.predictit.estimators.rasmussen
  (:require [hickory.core :as m]
            [hickory.select :as s]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as rcp-in])
  (:import [org.apache.commons.math3.stat.regression OLSMultipleLinearRegression]))

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
                (fn [date val] [date val])
                dates
                vals)]
        (into {} data)))

(defn get-rcp []
    (let [data (-> "rcp/6179_historical.json"
                    slurp
                    (utils/read-json {:value utils/to-decimal
                                      :date (comp #(.toLocalDate %) #(.atZone % utils/ny-time) utils/truncated-to-day utils/parse-rfc1123-datetime)})
                    :poll
                    :rcp_avg
                    (#(map rcp-in/to-canonical %))
                    (#(map
                        (fn [{:keys [approval date]}] [date approval])
                       %))
                    (#(into {} %)))]
        data))

(defn compute-lags [vals-by-date max-lag]
    (->> vals-by-date
         (map
            (fn [[date val]]
                [date
                    {:val val
                    :lags (->> (range 1 (inc max-lag))
                                (map
                                (fn [l] (get vals-by-date (.plusDays date (* -1 l)))))
                                (into []))}]))
         (map
            (fn [[date {:keys [val lags]}]]
                (if (every? some? lags)
                    [date
                     {:val val
                      :lags (map #(- val %) lags)}]
                    nil)))
         (filter some?)
         (into {})))

(defn get-data []
    (let [ras (compute-lags (get-ras) 3)
          rcp (compute-lags (get-rcp) 10)
          inputs (->> ras
                      (filter #(some? (get-in rcp [(.plusDays (first %) -1) :lags])))
                      (filter #(some? (get-in ras [(.plusDays (first %) -1) :lags])))
                      (map
                        (fn [[date {[val] :lags}]]
                            (let [yesterday (.plusDays date -1)]
                                (concat
                                    [val]
                                    [(get-in ras [yesterday :val])]
                                    (get-in rcp [yesterday :lags])
                                    (get-in ras [yesterday :lags]))))))
          y (double-array (map first inputs))
          x (into-array (map (comp double-array next) inputs))
          reg (OLSMultipleLinearRegression.)]
        (.newSampleData reg y x)
        {:params (->> (.estimateRegressionParameters reg)
                      seq
                      (into []))
         :params-stderr (->> (.estimateRegressionParametersStandardErrors reg)
                             seq
                             (into []))
         :rsq (.calculateRSquared reg)}))