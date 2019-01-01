(ns com.adamgberger.predictit.main
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.venues :as vs]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.strategies :as strats])
  (:gen-class))

(defn get-available-markets-chan [venue]
  (async/thread (v/available-markets venue)))

(defn maintain-venue-state [key interval-ms updater state end-chan]
  ; we kick off a go-routine that updates each venue's state
  ; at key with the result of (apply updater prev-state venue)
  ; every interval-ms and only stops when end-chan is closed

  (l/log :info "Starting venue maintenance" {:key key})

  ; TODO: This updates each venue serially.  Should fix.
  (let [{:keys [venues venue-state]} state
        upd (fn [venue-state venue]
              (let [id (v/id venue)
                    val (updater venue-state venue)]
                (assoc-in venue-state [id key] val)))
        handler #(doseq [venue venues]
                  (send-off venue-state upd venue))]
    (utils/repeatedly-until-closed
      handler
      600000
      #(l/log :warn "Stopping venue maintenance" {:key key})
      end-chan)))

(defn maintain-markets [state end-chan]
  ; we kick off a go-routine that polls all available markets in each venue
  ; and updates venue-state with the results.

  (letfn [(get-mkts [venue-state venue]
            (let [mkts (v/available-markets venue)]
              (l/log :info "Found markets for venue" {:venue-id (v/id venue)
                                                      :market-count (count mkts)})
              mkts))]
    (maintain-venue-state :mkts 600000 get-mkts state end-chan)))

(defn start-strategies [state end-chan]
  (map #(% state) (strats/all)))

(defn maintain-balances [state end-chan]
  ; we kick off a go-routine that polls balance in each venue
  ; and updates venue-state with the results.

  (letfn [(get-bal [venue-state venue]
            (let [bal (v/current-available-balance venue)]
              (l/log :info "Found balance for venue" {:venue-id (v/id venue)
                                                      :balance bal})
              bal))]
    (maintain-venue-state :bal 600000 get-bal state end-chan)))

(defn maintain-positions [state end-chan]
  ; we kick off a go-routine that polls balance in each venue
  ; and updates venue-state with the results.

  (letfn [(get-pos [venue-state venue]
            (let [pos (v/positions venue)]
              (l/log :info "Found positions for venue" {:venue-id (v/id venue)
                                                        :pos pos})
              pos))]
    (maintain-venue-state :pos 60000 get-pos state end-chan)))

(defn deref-map [m]
  (->> m
       (map (fn [[k v]]
          [k (if (= (type v) clojure.lang.Agent) @v v)]))
       (into {})))

(defn state-watchdog [state end-chan]
  ; print out our full state every once in a while

  (l/log :info "Starting state watchdog")

  (let [handler #(l/log :info "Current state" {:state (deref-map state)})]
    (utils/repeatedly-until-closed
      handler
      60000
      #(l/log :warn "Stopping state watchdog")
      end-chan)))

(defn run-trading [creds end-chan]
  (let [state {:venues (map #(% creds) (vs/venue-makers))
               :venue-state (agent {})
               :strats (agent {})
               :inputs (agent {})}]
    (start-strategies state end-chan)
    (maintain-markets state end-chan)
    (maintain-balances state end-chan)
    (maintain-positions state end-chan)
    (state-watchdog state end-chan)
    (async/<!! end-chan)))

(defn -main
  [& args]
  (let [end-chan (async/chan)]
    (run-trading {:email "adam.g.berger@gmail.com" :pwd ""} end-chan)))

(defn market-trader [market-id goal venue]
  ; start all inputs for the goal (inputs are order books, 3rd-party sources, etc)
  ; start all analyzers for the goal (analyzers take inputs and produce probability distributions across results and urgency scores)
  ; start all executors for the goal (executors take analyzer results, current positions, and an order book and enter trades)
  )