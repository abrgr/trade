(ns com.adamgberger.predictit.strategies
  (:require [com.adamgberger.predictit.strategies.approval-rating-rcp :as rcp])
  (:gen-class))

(defn all []
  [rcp/run])