(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:require [clojure.core.cache :as cache]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:import (com.jom OptimizationProblem DoubleMatrixND JOMException)
           (com.sun.jna Native Callback Callback$UncaughtExceptionHandler))
  (:gen-class))

; TODO: figure out how to stop ipopt from evaluating our objective function in the infeasible region
;       our variable bounds *nearly* rule this out
;       what is the behavior?  do we return an uninitialized objective function value or 0?
(let [default-exception-handler (Native/getCallbackExceptionHandler)]
  (Native/setCallbackExceptionHandler
    (reify Callback$UncaughtExceptionHandler
      (^void uncaughtException [this ^Callback c ^Throwable t]
        (when-not (and (instance? JOMException t)
                       (-> t (.getMessage) (.contains "Logarithms of non positive numbers are not defined")))
          (.uncaughtException default-exception-handler c t))))))

(defn- round [^long s n]
  (-> n
      bigdec
      (.setScale s java.math.RoundingMode/HALF_UP)
      double))

(def ^:private round2 (partial round 2))
(def ^:private round4 (partial round 4))

(defn- winner-matrix [contracts]
  (let [num-events (count contracts)
        num-bets (inc (* 2 num-events))
        M (make-array Double/TYPE num-events num-bets)]
    (doseq [event (range num-events)]
      (aset M event 0 1) ; cash
      (doseq [yes (range num-events)]
        (aset M event (inc yes) (if (= yes event) 1.0 0.0001))) ; yes bet on event is a winner, other yes's lose
      (doseq [no (range num-events)]
        (aset M event (+ num-events no 1) (if (= no event) 0.0001 1)))) ; our no loses, other nos win
      M))

(defn- prob-vector [contracts]
  (double-array (map :prob-yes contracts)))

(defn- odds-with-commission [price]
  (let [commission-rate 0.1
        gross-win (- 1.0 price)]
    (/ (- 1.0 (* commission-rate gross-win)) price)))

(defn- odds-matrix [contracts]
  (let [num-events (count contracts)
        num-bets (inc (* 2 num-events))
        o (make-array Double/TYPE num-events num-bets)]
    (doseq [event (range num-events)
            :let [{:keys [price-yes]} (get contracts event)]]
      ; cash returns 1x
      (aset o event 0 1)
      ; our yes returns our odds and all other yes's return 0
      (doseq [e (range num-events)]
        (aset o event (inc e) (if (= e event) (odds-with-commission price-yes) 0.0001)))
      ; our no returns 0 and all no's other than us return their odds
      (doseq [no (range num-events)
              :let [{:keys [price-no]} (get contracts no)]]
        (aset o event (+ num-events no 1) (if (= no event) 0.0001 (odds-with-commission price-no)))))
    o))

(defn- objective-fn [contracts]
  (let [num-events (count contracts)
        num-bets (inc (* 2 num-events))]
    (->> contracts
         (map-indexed
           (fn [i {:keys [prob]}]
             ["p(" i ") * ln("
              (->> num-bets
                   range
                   (map
                     (fn [j] (str "M(" i ", " j ") * x(" j ") * o(" i ", " j ")")))
                   (interpose " + "))
              ")"]))
         (interpose " + ")
         flatten
         (apply str))))

(defn- constraints [contracts]
  (let [var-for-idx #(str "x(" % ")")
        n-contracts (count contracts)
        vars (->> n-contracts (* 2) inc range (map var-for-idx))
        yes-vars (->> n-contracts inc (range 1) (map var-for-idx))
        no-vars (->> n-contracts (* 2) inc (range (inc n-contracts)) (map var-for-idx))]
    (->> vars
         (mapcat
           #(vector
              (str % " >= 0.000001")
              (str % " <= 1.000000")))
         ; only positive expectation bets: (map #(str "((prob(i) * o(i)) - 1 - hurdle_rate) * x(i) >= -0.0001") ...)
         (concat
           [(apply str (concat (->> vars (interpose " + ")) [" == 1"]))]
           (map
             #(str %1 " * " %2 " <= 0.01")
             yes-vars
             no-vars)))))

(defn- calc-prev-growth-rate [op x-mat prev-contracts]
  (if (nil? prev-contracts)
    nil
    (do
      (.setInputParameter op "o" (DoubleMatrixND. (odds-matrix prev-contracts) "dense"))
      (.set x-mat 0 (reduce - 1.0 (map :weight prev-contracts)))
      (doseq [[i c] (map-indexed vector prev-contracts)]
        (.set x-mat (inc i) (if (-> c :trade-type (= :buy-yes)) (:weight c) 0.0001))
        (.set x-mat (+ (count prev-contracts) 1 i) (if (-> c :trade-type (= :buy-no)) (:weight c) 0.0001)))
      (-> (.getObjectiveFunction op)
          (.evaluate (object-array ["x" x-mat]))
          (.get 0)
          Math/exp))))

(defn -bets-for-contracts [contracts prev-contracts]
  (try
    (let [op (OptimizationProblem.)]
      (.setInputParameter op "M" (DoubleMatrixND. (winner-matrix contracts) "dense"))
      (.setInputParameter op "p" (DoubleMatrixND. (prob-vector contracts) "row"))
      (.setInputParameter op "o" (DoubleMatrixND. (odds-matrix contracts) "dense"))
      (.addDecisionVariable op "x" false (int-array 1 [(-> contracts count (* 2) inc)]) 0.0001 1.0)
      (.setObjectiveFunction op "maximize" (objective-fn contracts))
      (doseq [c (constraints contracts)]
        (.addConstraint op c))
      (.solve op "ipopt" (object-array ["solverLibraryName" "ipopt"]))
      (let [n-contracts (count contracts)
            x-mat (.getPrimalSolution op "x")
            is-optimal (.solutionIsOptimal op)
            is-feasible (.solutionIsFeasible op)
            ok (and is-optimal is-feasible)
            prev-by-cid (->> prev-contracts
                             (group-by :contract-id)
                             (map (fn [[k v]] [k (first v)]))
                             (into {}))
            prev-contracts' (->> contracts
                                 (mapv
                                   #(let [c (get prev-by-cid (:contract-id %))]
                                     (merge % {:weight 0.0} (select-keys c [:price-yes :price-no :weight])))))]
        (if ok
            (let [growth-rate (-> (.getObjectiveFunction op)
                                  (.evaluate (object-array ["x" (.getPrimalSolution op "x")]))
                                  (.get 0)
                                  Math/exp)
                  prev-growth-rate (calc-prev-growth-rate op x-mat prev-contracts)]
              (if (or (nil? prev-growth-rate) (> growth-rate (+ 0.1 prev-growth-rate))) ; no changes unless we're improving by 10%
                {:bets (->> contracts
                            (map-indexed
                              (fn [i contract]
                                (let [yes-weight (.get x-mat (inc i))
                                      no-weight (.get x-mat (+ n-contracts 1 i))
                                      is-yes (> yes-weight no-weight)]
                                  (merge
                                    contract
                                    {:prob (if is-yes (:prob-yes contract) (- 1 (:prob-yes contract)))
                                     :est-value (if is-yes (:est-value-yes contract) (:est-value-no contract))
                                     :trade-type (if is-yes :buy-yes :buy-no)
                                     :price (if is-yes (:price-yes contract) (:price-no contract))
                                     :weight (-> (if is-yes yes-weight no-weight)
                                                 bigdec
                                                 (.setScale 2 java.math.RoundingMode/DOWN)
                                                 (double))}))))
                            (into []))
                 :growth-rate growth-rate
                 :prev-growth-rate prev-growth-rate}
                (do (l/log :info "Keeping prior contracts" {:growth-rate growth-rate :prev-growth-rate prev-growth-rate})
                    {:bets prev-contracts'
                     :growth-rate prev-growth-rate
                     :prev-growth-rate prev-growth-rate})))
            {:error (ex-info "Bad optimization solution" {:anomaly :bad-optimization :optimal? is-optimal :feasible? is-feasible :contracts contracts})})))
    (catch Exception e
      {:error e})))

(defn- -get-optimal-bets
  ([hurdle-return contracts prev-contracts]
    (-get-optimal-bets hurdle-return contracts prev-contracts false))
  ([hurdle-return contracts prev-contracts is-retry]
    (let [{:keys [bets growth-rate prev-growth-rate error]} (-bets-for-contracts contracts prev-contracts)]
      (if (some? error)
        (do (l/log :error "Failed to optimize portfolio" {:contracts contracts
                                                          :opt-result error
                                                          :will-retry (not is-retry)})
            (when-not is-retry
              (-get-optimal-bets
                hurdle-return
                ; try without well-priced "definitely-no" contracts
                (filterv #(not (and (< (Math/abs (- (:price-yes %) (:prob-yes %))) 0.02)
                                    (< (Math/abs (- (:price-no %) (- 1 (:prob-yes %)))) 0.02)
                                    (< (:prob-yes %) 0.02)))
                         contracts)
                prev-contracts
                true)))
        (do
          (l/log :info "Optimized portfolio" {:contracts contracts
                                              :result bets
                                              :growth-rate growth-rate
                                              :prev-growth-rate prev-growth-rate})
          (filterv
            #(> (:weight %) 0.01)
            bets))))))

; memoize since we're doing many runs of an expensive operation
(def ^:private bet-cache (atom (cache/lru-cache-factory {} :threshold 100)))

(defn get-optimal-bets
  ([hurdle-return contracts-price-and-prob]
    (get-optimal-bets hurdle-return contracts-price-and-prob nil))
  ([hurdle-return contracts-price-and-prob prev-contracts]
    (let [rounded-contracts (transduce
                              (comp
                                (map #(update % :prob-yes (comp round4 (partial max 0.0001))))
                                (map #(update % :est-value-yes (comp round4 (partial max 0.0001))))
                                (map #(update % :est-value-no (comp round4 (partial max 0.0001))))
                                (map #(update % :price-yes (comp round2 (partial max 0.01))))
                                (map #(update % :price-no (comp round2 (partial max 0.01)))))
                              conj
                              contracts-price-and-prob)
          k [hurdle-return rounded-contracts]
          bet-cache' (cache/through #(-get-optimal-bets (first %) (second %) prev-contracts) @bet-cache k)
          result (cache/lookup bet-cache' k)]
      (comment (when (some? result)
        ; we don't cache errors
        (reset! bet-cache bet-cache'))) ; may drop some cached items if we're running concurrently.  we shouldn't be running concurrently
      result)))
