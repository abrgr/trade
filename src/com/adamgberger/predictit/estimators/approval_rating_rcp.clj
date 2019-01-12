(ns com.adamgberger.predictit.estimators.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as approval-rcp]
            [com.adamgberger.predictit.inputs.rasmussen :as approval-rasmussen]
            [com.adamgberger.predictit.inputs.yougov-weekly-registered :as approval-yougov-weekly-registered])
  (:gen-class))

(def id ::id)

(defn update-est [rcp rasmussen yougov state]
  (when (some? rcp)
    (let [c (:constituents rcp)
          with-yougov (if (some? yougov)
                        (assoc
                          c
                          "Economist/YouGov"
                          {:val (:val yougov)
                           :start (:date yougov)
                           :end (:date yougov)})
                        c)
          with-rasmussen (if (some? rasmussen)
                            (assoc
                              with-yougov
                              "Rasmussen Reports"
                              {:val (:val rasmussen)
                              :start (:date rasmussen)
                              :end (:date rasmussen)})
                            with-yougov)
          new-constituents with-rasmussen
          est (-> new-constituents
                  approval-rcp/recalculate-average
                  :exact)]
      (send (:estimates state) #(assoc
                                  %
                                  id
                                  {:val est
                                   :est-at (java.time.Instant/now)
                                   :date (:date rcp)
                                   :diff (- est (:val rcp))})))))

(defn run [state end-chan]
  (l/log :info "Starting RCP approval estimator")
  (let [c (async/chan)]
    (async/go-loop []
      (let [[[rcp rasmussen yougov] ch] (async/alts! [c end-chan])]
            ; TODO: add some checks to make sure we don't overwrite a good value (basically, check our other inputs)
        (if (= ch end-chan)
          (l/log :warn "Stopping RCP approval estimator")
          (do (update-est rcp rasmussen yougov state)
              (recur)))))
    (utils/add-guarded-watch-ins
        (:inputs state)
        ::run
        [[approval-rcp/id]
         [approval-rasmussen/id]
         [approval-yougov-weekly-registered/id]]
        #(and (not= %1 %2) (some? %2))
        #(async/>!! c %))))