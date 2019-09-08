(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:require [clojure.core.cache :as cache]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:import (com.jom OptimizationProblem DoubleMatrixND))
  (:gen-class))

(def ^:private optimizer-runs 1000)

(defn- odds [pos result]
  (let [{:keys [price trade-type cash?]} pos
        price (when (some? price) (double price))
        result-yes? (= result :yes)
        bet-yes? (= trade-type :buy-yes)
        correct-bet? (= result-yes? bet-yes?)]
    (cond
      cash? 0
      correct-bet? (/ 1 price)
      :else -1)))

(defn- wager-result [pos result]
  (let [{:keys [^Double wager]} pos
        b (odds pos result)]
    (* b (Math/abs wager))))

(defn- growth-for-yes-pos [contract-positions pos]
  (let [g (reduce
           (fn [sum this-pos]
             (let [side (if (= this-pos pos) :yes :no)
                   res (wager-result this-pos side)]
               (+ sum res)))
           1.0
           contract-positions)]
    (Math/log (max g Double/MIN_NORMAL)))) ; the max is here since we may not meet the budget constraint so may be negative

(defn- growth-rate [contract-positions]
  ; This is adapted from Thorpe's discussion of the Kelly Criterion
  ; http://www.eecs.harvard.edu/cs286r/courses/fall12/papers/Thorpe_KellyCriterion2007.pdf
  (reduce
   (fn [sum {:keys [prob] :as pos}]
     (let [exp-growth (* prob (growth-for-yes-pos contract-positions pos))]
       (+ sum exp-growth)))
   0.0
   contract-positions))

(defn- exp-return [{:keys [est-value price]}]
  (/ (- est-value price) price))

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
      (aset M event (inc event) 1) ; yes bet on event is a winner
      (doseq [no (range num-events)
              :when (not= no event)]
        (aset M event (+ num-events no 1) 1))) ; other nos win
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
            :let [{:keys [price-yes price-no]} (get contracts event)]]
      (aset o event 0 1) ; cash returns 1x
      (aset o event (inc event) (odds-with-commission price-yes))
      (doseq [no (range num-events)
              :when (not= no event)]
        (aset o no (+ num-events event 1) (odds-with-commission price-no))))
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
             #(str %1 " * " %2 " <= 0.0001")
             yes-vars
             no-vars)))))

(defn -bets-for-contracts [contracts]
  (try
    (let [op (OptimizationProblem.)]
      (.setInputParameter op "M" (DoubleMatrixND. (winner-matrix contracts) "dense"))
      (.setInputParameter op "p" (DoubleMatrixND. (prob-vector contracts) "row"))
      (.setInputParameter op "o" (DoubleMatrixND. (odds-matrix contracts) "dense"))
      (.addDecisionVariable op "x" false (int-array 1 [(-> contracts count (* 2) inc)]) 0.000001 1.0)
      (.setObjectiveFunction op "maximize" (objective-fn contracts))
      (doseq [c (constraints contracts)]
        (.addConstraint op c))
      (.solve op "ipopt" (into-array ["solverLibraryName" "ipopt"]))
      (let [n-contracts (count contracts)
            x-mat (.getPrimalSolution op "x")
            is-optimal (.solutionIsOptimal op)
            is-feasible (.solutionIsFeasible op)
            ok (and is-optimal is-feasible)]
        (if ok
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
                                             (double))})))))
             :growth-rate (-> (.getObjectiveFunction op)
                              (.evaluate (object-array ["x" (.getPrimalSolution op "x")]))
                              (.get 0)
                              Math/exp)}
            {:error (ex-info "Bad optimization solution" {:optimal? is-optimal :feasible? is-feasible :contracts contracts})})))
    (catch Exception e
      {:error e})))

(defn- -get-optimal-bets [hurdle-return contracts prev-weights-by-contract-id]
  (let [{:keys [bets growth-rate error]} (-bets-for-contracts contracts)]
    (if (some? error)
      (do (l/log :error "Failed to optimize portfolio" {:contracts contracts
                                                        :opt-result error})
          nil)
      (do
        (l/log :info "Optimized portfolio" {:contracts contracts
                                            :result bets
                                            :growth-rate growth-rate})
        (filterv
          #(> (:weight %) 0.01)
          bets)))))

; memoize since we're doing many runs of an expensive operation
(def ^:private bet-cache (atom (cache/lru-cache-factory {} :threshold 100)))

(defn get-optimal-bets
  ([hurdle-return contracts-price-and-prob]
    (get-optimal-bets hurdle-return contracts-price-and-prob nil))
  ([hurdle-return contracts-price-and-prob prev-weights-by-contract-id]
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
          bet-cache' (cache/through #(-get-optimal-bets (first %) (second %) prev-weights-by-contract-id) @bet-cache k)
          result (cache/lookup bet-cache' k)]
      (when (some? result)
        ; we don't cache errors
        (reset! bet-cache bet-cache')) ; may drop some cached items if we're running concurrently.  we shouldn't be running concurrently
      result)))
