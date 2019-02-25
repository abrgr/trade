(ns com.adamgberger.predictit.estimators.approval-rating-rcp
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.inputs.approval-rating-rcp :as approval-rcp]
            [com.adamgberger.predictit.inputs.rasmussen :as approval-rasmussen]
            [com.adamgberger.predictit.inputs.yougov-weekly-registered :as approval-yougov-weekly-registered]
            [com.adamgberger.predictit.inputs.approval-rating-the-hill :as approval-the-hill])
  (:import (org.apache.commons.math3.stat.descriptive DescriptiveStatistics))
  (:gen-class))

(def id ::id)

(defn- dist-for-same-day [dist-by-days est primary-sources]
  (if (every? some? primary-sources)
    (let [tz utils/ny-time
          now (java.time.ZonedDateTime/now tz)
          end-of-day (-> now
                        (.withHour 23)
                        (.withMinute 59))
          remaining-for-today (->> primary-sources
                                  (filter #(< (compare (:next-expected %) end-of-day) 0)))
          remaining-count (count remaining-for-today)
          pct-left (double (/ remaining-count (count primary-sources)))
          {:keys [mean std]} (get dist-by-days 1)
          adj-mean (* mean pct-left)
          adj-std (* std pct-left)]
      (if (= 0 remaining-count)
        (org.apache.commons.math3.distribution.ConstantRealDistribution. est) ; TODO: this isn't quite right - poisson of whether rcp actually updates + possibility of unknown surveys
        (org.apache.commons.math3.distribution.NormalDistribution. (+ est adj-mean) adj-std)))
    nil))

(defn- dist-for-future-day [days dist-by-days est]
  (let [tz utils/ny-time
        now (java.time.ZonedDateTime/now tz)
        start-of-day (-> now
                         .toLocalDate
                         (.atStartOfDay tz))
        end-of-day (-> now
                       .toLocalDate
                       (.plus 1 java.time.temporal.ChronoUnit/DAYS)
                       (.atStartOfDay tz))
        elapsed-seconds (.until now end-of-day java.time.temporal.ChronoUnit/SECONDS)
        total-seconds (.until start-of-day end-of-day java.time.temporal.ChronoUnit/SECONDS)
        next-ratio (double (/ elapsed-seconds total-seconds))
        cur-ratio (- 1 next-ratio)
        {:keys [mean std]} (get dist-by-days days)
        {next-mean :mean next-std :std} (get dist-by-days (dec days))
        ; we take the weighted average of the two days
        ; TODO: could be smarter and do this based on # of expected datapoints already received
        adj-mean (+ (* mean cur-ratio) (* next-mean next-ratio))
        adj-std (+ (* std cur-ratio) (* next-std next-ratio))]
    (org.apache.commons.math3.distribution.NormalDistribution. (+ est adj-mean) adj-std)))

(defn- dist-for-days [days dist-by-days est primary-sources]
  (if (= days 0)
    (dist-for-same-day dist-by-days est primary-sources)
    (dist-for-future-day days dist-by-days est)))

(defn- calc-dist-by-day
  [vals days]
  (let [^DescriptiveStatistics desc (->> (partition (inc days) 1 vals)
                                         (map
                                            #(- (-> % first :approval) (-> % last :approval)))
                                         double-array
                                         DescriptiveStatistics.)]
    {:mean (.getMean desc)
     :std (.getStandardDeviation desc)}))

(defn- calc-dist-by-days
  [stats-for-days {:keys [vals]} days]
  (let [vs (->> vals (take stats-for-days) (into []))
        dists (->> (range 1 (inc days))
                   (map
                      (fn [d]
                        [d (calc-dist-by-day vs d)]))
                   (into {}))
        {mean-1 :mean std-1 :std} (get dists 1)]
    ; TODO: this is really how I'm trading??
    (assoc
      dists
      0
      {:mean (* mean-1 0.75)
       :std (* std-1 0.75)})))

(defn- update-est [stats-for-days rcp rcp-hist rasmussen yougov thehill state]
  (when (and (some? rcp) (some? rcp-hist))
    (let [{c :constituents
           valid? :valid?
           rcp-val :val
           rcp-date :date} rcp
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
          final (if (some? thehill)
                    (assoc
                      with-rasmussen
                      "The Hill/HarrisX"
                      {:val (:val thehill)
                       :start (:date thehill)
                       :end (:date thehill)}))
          new-constituents final
          est (if valid?
                (-> new-constituents
                    approval-rcp/recalculate-average
                    :exact)
                rcp-val) ; there was some problem with a consistency check on recalculating rcp, just use the given value
          dist-by-days (calc-dist-by-days stats-for-days rcp-hist 7)
          ests (->> dist-by-days
                    (map
                      (fn [[days {:keys [mean std]}]]
                        [days (dist-for-days days dist-by-days est [yougov rasmussen])]))
                    (into {}))]
      (send (:estimates state) #(assoc
                                  %
                                  id
                                  {:val est
                                   :dists-by-day ests
                                   :est-at (java.time.Instant/now)
                                   :date rcp-date
                                   :diff (- est rcp-val)})))))

(defn run [cfg state end-chan]
  (l/log :info "Starting RCP approval estimator")
  (let [c (async/chan)
        our-cfg (::id cfg)
        {:keys [stats-for-days]} our-cfg]
    (when (nil? stats-for-days)
      (throw (ex-info "Bad config for rcp estimator" {:cfg our-cfg})))
    (async/go-loop []
      (let [[[rcp rcp-hist rasmussen yougov the-hill] ch] (async/alts! [c end-chan])]
            ; TODO: add some checks to make sure we don't overwrite a good value (basically, check our other inputs)
        (if (= ch end-chan)
          (l/log :warn "Stopping RCP approval estimator")
          (do (update-est stats-for-days rcp rcp-hist rasmussen yougov the-hill state)
              (recur)))))
    (utils/add-guarded-watch-ins
        (:inputs state)
        ::run
        [[approval-rcp/id]
         [approval-rcp/hist]
         [approval-rasmussen/id]
         [approval-yougov-weekly-registered/id]
         [approval-the-hill/id]]
        #(and (not= %1 %2) (some? %2))
        #(async/>!! c %))))