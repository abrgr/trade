(ns com.adamgberger.predictit.strategies.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(defn is-relevant-mkt [mkt]
    true)

(defn maintain-relevant-mkts [state end-chan]
  (l/log :info "Starting relevant market maintenance for RCP")
  nil)

(defn nope [all rel end-chan]
  (let [rel-chan (async/chan)]
    ; kick off a go routine to filter markets down to relevant markets
    ; and update the rel channel with the results
    (async/go-loop []
      (let [[val _] (async/alts! [rel-chan end-chan])]
        (println "GOT IT" val)
        (when (some? val)
          (let [rel-mkts (filter is-relevant-mkt val)]
            (l/log :info "Updating relevant markets" {:rel-count (count rel-mkts)
                                                      :total-count (count val)})
            (send rel (constantly rel-mkts))
            (recur)))))
    ; watch for changes to the all agent and send new values to our go routine
    (add-watch all :rel #(async/>!! rel-chan %4))))

(defn run [state end-chan]
    (maintain-relevant-mkts state end-chan)
    (async/<!! end-chan))