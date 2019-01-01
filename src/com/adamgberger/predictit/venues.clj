(ns com.adamgberger.predictit.venues
  (:require [com.adamgberger.predictit.venues.predictit :as pvenue])
  (:gen-class))

(defn venue-makers []
    [pvenue/make-venue])