(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:require [clojure.core.cache :as cache]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:import (de.xypron.jcobyla Calcfc
                              Cobyla
                              CobylaExitStatus)
           (com.jom OptimizationProblem))
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
  (double-array (concat (map :prob contracts) (map #(- 1 (:prob %)) contracts))))

(defn- odds-matrix [contracts]
  (let [num-events (count contracts)
        num-bets (inc (* 2 num-events))
        o (make-array Double/TYPE num-events num-bets)]
    (doseq [event (range num-events)
            :let [{:keys [price]} (get contracts event)]]
      (aset o event 0 1) ; cash returns 1x
      (aset o event (inc event) (/ 1 price)) ; yes bet on event wins 1/price
      (doseq [no (range num-events)
              :when (not= no event)]
        (aset o no (+ num-events event 1) (/ 1 (- 1 price))))) ; no bet on event wins 1/(1-price)
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
  (let [vars (->> contracts count (* 2) inc range (map #(str "x(" % ")")))]
    (->> vars
         (mapcat
           #(vector
              (str % " >= 0.0001")
              (str % " <= 1.0000")))
         (concat
           [(apply str (concat (->> vars (interpose " + ")) [" == 1"]))]))))

(defn -bets-for-contracts
  ([contracts]
    (-bets-for-contracts contracts (for [_ contracts] (rand))))
  ([contracts weight-seq]
    (let [op (OptimizationProblem.)]
      (.setInputParameter op "M" (DoubleMatrixND. (winner-matrix contracts)))
      (.setInputParameter op "p" (DoubleMatrixND. (prob-vector contracts) "row"))
      (.setInputParameter op "o" (DoubleMatrixND. (odds-matrix contracts)))
      (.addDecisionVariable op "x" false (int-array 1 [(-> contracts count (* 2) inc)]) 0.0 1.0)
      (.setObjectiveFunction op "maximize" (objective-fn contracts))
      (doseq [c (constraints contracts)]
        (.addConstraint op c))
      (.solve op "ipopt" (into-array ["solverLibraryName" "ipopt"]))
      (.solutionIsOptimal op)
      (let [x (.getPrimalSolution op "x")])
      op)
      ; TODO: right now, "no" contracts are passed in with :prob and :price set to the no values, need to fix this in prob-vector and odds-matrix
      ; TODO: odds should be set taking commission into account.  not 1 / price but something like (1 - (0.1 * (1-price)))/price
      ; TODO: convert back to expected output 
    ))

    (let [len (count contracts)
          weights (double-array len weight-seq)
          ; constraints are represented as functions that must be non-negative
          non-neg-constraints (mapv
                               (fn [^Integer i]
                                 (fn [^doubles x ^doubles con]
                                   (aset-double con (inc i) (nth x i)))) ; (inc i) because budget constraint is 0th constraint
                               (range len))
          budget-constraint (fn [^doubles x ^doubles con]
                              (aset-double
                               con
                               0
                               (- 1 (reduce (fn [^Double sum ^Double val] (+ sum (Math/abs val))) 0.0 x))))
          constraints (conj non-neg-constraints budget-constraint)
          num-constraints (count constraints)
          f (reify Calcfc
              (compute [this n m x con]
                (doseq [constraint constraints] (constraint x con)) ; constraints mutate values of con
                (* -1 (growth-rate (mapv #(assoc %1 :wager %2) contracts x)))))
          rho-start 0.25
          rho-end 1.0e-3
          res (Cobyla/findMinimum f len num-constraints weights rho-start rho-end 0 1e8)]
      (if (= CobylaExitStatus/NORMAL res)
        (let [with-weights (transduce
                             (comp
                               (map-indexed #(assoc (get contracts %1) :weight (round2 (max 0 %2)))) ; need the max here to get rid of double artifacts that break our non-neg constraint
                               (filter (comp not :cash?)))
                             conj
                             weights)]
          {:result with-weights})
        {:error res}))))

(defn- -get-optimal-bets [hurdle-return contracts-price-and-prob prev-weights-by-contract-id]
  (let [filtered-contracts (transduce
                             (comp
                                (filter #(> (exp-return %) hurdle-return))
                                (map
                                  #(u/map-xform
                                     %
                                     {:prob round2
                                      :est-value round2
                                      :price (comp bigdec round2)})))
                             conj
                             contracts-price-and-prob)
        total-prob (->> filtered-contracts
                        (map :prob)
                        (reduce + 0.0))
        remaining-prob (- 1 total-prob)
        contracts (if (> remaining-prob 0.0)
                    (conj filtered-contracts {:prob remaining-prob :cash? true})
                    filtered-contracts)
        result-growth (fn [{bets :result :keys [error]}]
                        (if (some? error)
                            -100
                            (growth-rate (map #(assoc % :wager (:weight %)) bets))))
        ; TODO: it would be great to find a proper constrained optimizer for differentiable functions
        ;       as-is, we are meta-optimizing our optimizer
        max-result (reduce
                     (fn [best {bets :result :as opt-result}]
                       (let [growth-rate (result-growth opt-result)]
                         (if (> growth-rate (:growth-rate best))
                           {:growth-rate growth-rate
                            :bets bets}
                           best)))
                     {:growth-rate -1
                      :error (ex-info "Could not optimize portfolio" {:anomaly :failed-optimization})}
                     (concat
                       ; we always want to make sure we try one run starting from our last optimized value
                       (let [weights (mapv #(get prev-weights-by-contract-id (:contract-id %) 0.0) contracts)]
                         [(-bets-for-contracts contracts weights)])
                       (pmap (fn [_] (-bets-for-contracts contracts)) (range optimizer-runs))))
        fp-result (-bets-for-contracts contracts (mapv :weight (-> max-result :bets)))
        fp-result-growth (result-growth fp-result)
        result (if (> fp-result-growth (:growth-rate max-result))
                   (merge fp-result {:bets (:result fp-result)})
                   max-result)
        {:keys [bets error]} result]
    (if (some? error)
      (do (l/log :error "Failed to optimize portfolio" {:contracts contracts
                                                        :orig-contracts contracts-price-and-prob
                                                        :opt-result error})
          nil)
      (do
        (l/log :info "Optimized portfolio" {:contracts contracts
                                            :orig-contracts contracts-price-and-prob
                                            :result bets
                                            :opt-result CobylaExitStatus/NORMAL})
        bets))))

; memoize since we're doing many runs of an expensive operation
(def ^:private bet-cache (atom (cache/lru-cache-factory {} :threshold 100)))

(defn get-optimal-bets
  ([hurdle-return contracts-price-and-prob]
    (get-optimal-bets hurdle-return contracts-price-and-prob nil))
  ([hurdle-return contracts-price-and-prob prev-weights-by-contract-id]
    (let [rounded-contracts (transduce
                              (comp
                                (map #(update % :prob round4))
                                (map #(update % :est-value round4)))
                              conj
                              contracts-price-and-prob)
          k [hurdle-return rounded-contracts]
          bet-cache' (cache/through #(-get-optimal-bets (first %) (second %) prev-weights-by-contract-id) @bet-cache k)
          result (cache/lookup bet-cache' k)]
      (when (some? result)
        ; we don't cache errors
        (reset! bet-cache bet-cache')) ; may drop some cached items if we're running concurrently.  we shouldn't be running concurrently
      result)))
