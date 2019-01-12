(ns com.adamgberger.predictit.inputs
  (:require [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as approval-rcp]
            [com.adamgberger.predictit.inputs.approval-rating-538:as approval-538]
            [com.adamgberger.predictit.inputs.harris-interactive :as approval-harris-interactive]
            [com.adamgberger.predictit.inputs.rasmussen :as approval-rasmussen]
            [com.adamgberger.predictit.inputs.yougov-weekly-registered :as approval-yougov-weekly-registered])
  (:gen-class))

(defn update-input [state input-id input-val]
    (send
        (:inputs state)
        #(assoc %1 input-id input-val)))

(defn run-input [state end-chan input-id input]
    (l/log :info "Starting input" {:input-id input-id})
    (utils/repeatedly-until-closed
        #(input (partial update-input state input-id))
        10000 ; 10 seconds
        #(l/log :info "Stopping input" {:input-id input-id})
        end-chan))

(def inputs
    {approval-rcp/id approval-rcp/get-current
     approval-rasmussen/id approval-rasmussen/get-current
     approval-yougov-weekly-registered/id approval-yougov-weekly-registered/get-current
     ;approval-harris-interactive/id approval-harris-interactive/get-current
     ;approval-538/id approval-538/get-current
     })

(defn start-all [state end-chan]
    (doseq [[id input] inputs]
        (run-input state end-chan id input)))