(ns com.adamgberger.predictit.main
  (:require [clojure.core.async :as async]
            [com.adamgberger.predictit.lib.utils :as utils]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.venues :as vs]
            [com.adamgberger.predictit.venues.venue :as v]
            [com.adamgberger.predictit.strategies :as strats]
            [com.adamgberger.predictit.inputs :as inputs])
  (:gen-class))

(defn get-available-markets-chan [venue]
  (async/thread (v/available-markets venue)))

(defn maintain-venue-state [key interval-ms updater state end-chan]
  ; we kick off a go-routine that updates each venue's state
  ; at key with the result of (updater venue)
  ; every interval-ms and only stops when end-chan is closed

  (l/log :info "Starting venue maintenance" {:key key})

  (let [{:keys [venues venue-state]} state
        upd (fn [venue-state id val]
              (assoc-in venue-state [id key] val))
        handler #(doseq [venue venues]
                    (let [val (updater venue)
                          id (v/id venue)]
                      (send-off venue-state upd id val)))]
    (utils/repeatedly-until-closed
      handler
      600000
      #(l/log :warn "Stopping venue maintenance" {:key key})
      end-chan)))

(defn maintain-markets [state end-chan]
  ; we kick off a go-routine that polls all available markets in each venue
  ; and updates venue-state with the results.

  (letfn [(get-mkts [venue]
            (let [mkts (v/available-markets venue)]
              (l/log :info "Found markets for venue" {:venue-id (v/id venue)
                                                      :market-count (count mkts)})
              mkts))]
    (maintain-venue-state :mkts 600000 get-mkts state end-chan)))

(defn start-strategies [state end-chan]
  (reduce
    (fn [strats next]
      (merge strats (next state end-chan)))
    {}
    (strats/all)))

(defn maintain-balances [state end-chan]
  ; we kick off a go-routine that polls balance in each venue
  ; and updates venue-state with the results.

  (letfn [(get-bal [venue]
            (let [bal (v/current-available-balance venue)]
              (l/log :info "Found balance for venue" {:venue-id (v/id venue)
                                                      :balance bal})
              bal))]
    (maintain-venue-state :bal 600000 get-bal state end-chan)))

(defn maintain-positions [state end-chan]
  ; we kick off a go-routine that polls balance in each venue
  ; and updates venue-state with the results.

  (letfn [(get-pos [venue]
            (let [pos (v/positions venue)]
              (l/log :info "Found positions for venue" {:venue-id (v/id venue)
                                                        :pos pos})
              pos))]
    (maintain-venue-state :pos 60000 get-pos state end-chan)))

(defn deref-map [m]
  (->> m
       (map (fn [[k v]]
          [k
           (cond
            (= (type v) clojure.lang.Agent) @v
            (map? v) (deref-map v)
            :else v)]))
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

(defn update-order-book [state venue-id market-id contract val]
  (l/log :info "Received order book update" {:venue-id venue-id
                                             :market-id market-id
                                             :contract-id (:contract-id contract)})
  (send
    (:venue-state state)
    #(assoc-in
      %
      [venue-id :order-books market-id (:contract-id contract)]
      {:contract contract
       :book val})))

(defn all-order-book-reqs [venue-state venue-id]
  (->> (get-in venue-state [venue-id :req-order-books])
       vals
       (apply concat)
       (into #{})))

(defn continue-monitoring-order-book [state venue-id market-id contract-id]
  (let [venue-state @(:venue-state state)
        req-books (all-order-book-reqs venue-state venue-id)]
    (->> req-books
         (filter #(= market-id (:market-id %)))
         empty?
         not)))

(defn maintain-order-book [state end-chan venue-id venue req]
  (l/log :info "Starting watch of order book" {:venue-id venue-id
                                               :req req})
  (let [{:keys [market-id market-url]} req
        contracts (v/contracts venue market-id market-url)]
    (doseq [contract contracts]
      (send
        (:venue-state state)
        #(assoc-in % [venue-id :order-books market-id (:contract-id contract) :contract contract] contract))
    (v/monitor-order-book
      venue
      market-id
        market-url
        (:contract-id contract)
        (partial update-order-book state venue-id market-id contract)
        (partial continue-monitoring-order-book state venue-id)))))

(defn maintain-order-books [state end-chan]
  ; monitor state -> venue-state -> venue-id -> :req-order-books -> strat-id for sets of maps like this:
  ; {:market-id x :market-name x :contract-id y}
  (add-watch
    (:venue-state state)
    ::maintain-order-books
    (fn [_ _ old new]
      (let [venue-ids (set (concat (keys old) (keys new)))]
        (doseq [venue-id venue-ids]
          (let [new-reqs (all-order-book-reqs new venue-id)
                old-reqs (all-order-book-reqs old venue-id)
                added (clojure.set/difference new-reqs old-reqs)
                venue (->> state
                           :venues
                           (filter #(= venue-id (v/id %)))
                           first)]
            (doseq [to-add added]
              (maintain-order-book state end-chan venue-id venue to-add))))))))

(defn run-trading [creds end-chan]
  (let [state {:venues (map #(% creds) (vs/venue-makers))
               :venue-state (agent {})
               :strats {}
               :inputs (agent {})}
        state (assoc-in state [:strats] (start-strategies state end-chan))]
    (maintain-markets state end-chan)
    (maintain-balances state end-chan)
    (maintain-positions state end-chan)
    (maintain-order-books state end-chan)
    (inputs/start-all state end-chan)
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