(ns com.adamgberger.predictit.inputs
  (:require [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as approval-rcp]
            [com.adamgberger.predictit.inputs.approval-rating-538 :as approval-538])
  (:gen-class))

(defn update-input [state input-id input]
    (let [input-val (input (java.time.Instant/now))]
        (send
            (:inputs state)
            #(assoc-in %1 [input-id] input-val))))

(defn run-input [state end-chan input-id input]
    (l/log :info "Starting input" {:input-id input-id})
    (utils/repeatedly-until-closed
        (partial update-input state input-id input)
        10000 ; 10 seconds
        #(l/log :info "Stopping input" {:input-id input-id})
        end-chan))

(def inputs
    {approval-rcp/id approval-rcp/get-current
     approval-538/id approval-538/get-current})

(defn start-all [state end-chan]
    (doseq [[id input] inputs]
        (run-input state end-chan id input)))