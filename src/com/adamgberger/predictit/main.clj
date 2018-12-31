(ns com.adamgberger.predictit.main
  (:require [com.adamgberger.predictit.apis.predictit :as p])
  (:gen-class))

(defn -main
  [& args]
  (p/monitor-order-book {} 5124 "@realDonaldTrump-tweets-12-26-1-2" 8929 #(println "UPDATE!" %)))
