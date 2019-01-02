(ns com.adamgberger.predictit.strategies.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(def venue-id :com.adamgberger.predictit.venues.predictit/predictit)

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
          (let [rel-mkts (->> mkts
                              (filter is-relevant-mkt)
                              (into []))]
            (l/log :info "Updating relevant markets for RCP" {:rel-count (count rel-mkts)
                                                              :total-count (count mkts)})
            (send strat-state #(merge % {:mkts rel-mkts}))
            (recur)))))
    ; watch for changes to predictit markets and send new values to our go routine
    (utils/add-guarded-watch-in
        (:venue-state state)
        ::rel-mkts
        [venue-id :mkts]
        #(and (not= %1 %2) (some? %2))
        #(async/>!! rel-chan %))))

(defn maintain-order-books [state strat-state end-chan]
    (utils/add-guarded-watch-in
        strat-state
        ::maintain-order-books
        [:mkts]
        not=
        (fn [mkts]
            (let [needed-order-books (->> mkts
                                          (map #(select-keys % [:market-id :market-url :market-name]))
                                          (into #{}))
                  updater #(assoc-in % [venue-id :req-order-books ::id] needed-order-books)
                  venue-state (:venue-state state)]
                (l/log :info "Requesting order books" {:needed-order-books needed-order-books})
                (send venue-state updater)))))

(defn run [state end-chan]
    (let [strat-state (agent {})]
        (maintain-relevant-mkts state strat-state end-chan)
        (maintain-order-books state strat-state end-chan)
        {::state strat-state}))