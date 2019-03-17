(ns com.adamgberger.predictit.estimators
  (:require [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.estimators.approval-rating-rcp :as approval-rcp])
  (:gen-class))

(def estimators
  {approval-rcp/id approval-rcp/run})

(defn start-all [cfg state end-chan]
  (doseq [[id estimator] estimators]
    (estimator cfg state end-chan)))