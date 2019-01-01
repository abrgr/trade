(ns com.adamgberger.predictit.strategies.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(defn is-relevant-mkt [mkt]
    (let [n (-> mkt :market-name .toLowerCase)
          is-open (= (:status mkt) :open)
          is-trump (.contains n "trump")
          is-rcp (.contains n " rcp ")
          is-approval (.contains n " approval ")]
        (and is-open is-trump is-rcp is-approval)))

(defn maintain-relevant-mkts [state strat-state end-chan]
  (l/log :info "Starting relevant market maintenance for RCP")
  (let [rel-chan (async/chan)]
    ; kick off a go routine to filter markets down to relevant markets
    ; and update the strategy with the results
    (async/go-loop []
      (let [[mkts _] (async/alts! [rel-chan end-chan])]
        (when (some? mkts)
          (let [rel-mkts (filter is-relevant-mkt mkts)]
            (l/log :info "Updating relevant markets for RCP" {:rel-count (count rel-mkts)
                                                              :total-count (count mkts)})
            (send strat-state #(merge % {:mkts rel-mkts}))
            (recur)))))
    ; watch for changes to predictit markets and send new values to our go routine
    (add-watch
        (:venue-state state)
        ::rel-mkts
        #(->> %4
              :com.adamgberger.predictit.venues.predictit/predictit
              :mkts
              (async/>!! rel-chan)))))

(defn run [state end-chan]
    (let [strat-state (agent {})]
        (maintain-relevant-mkts state strat-state end-chan)
        {::state strat-state}))