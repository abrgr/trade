(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:require [clojure.core.memoize :as memo]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:import (de.xypron.jcobyla Calcfc
                              Cobyla
                              CobylaExitStatus))
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

(declare -get-optimal-bets)

(defn- -bets-for-contracts
  ([contracts]
    (let [len (count contracts)]
      (-bets-for-contracts contracts (double-array len (map (fn [_] (rand)) (range len))))))
  ([contracts weights]
    (let [len (count contracts)
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

(defn- -get-optimal-bets [hurdle-return contracts-price-and-prob]
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
        ; TODO: it would be great to find a proper constrained optimizer for differentiable functions
        ;       as-is, we are meta-optimizing our optimizer
        result (reduce
                 (fn [best {bets :result :keys [error]}]
                   (let [growth-rate (if (some? error)
                                         -100
                                         (growth-rate (map #(assoc % :wager (:weight %)) bets)))]
                     (if (> growth-rate (:growth-rate best))
                       {:growth-rate growth-rate
                        :bets bets}
                       best)))
                 {:growth-rate -1
                  :error (ex-info "Could not optimize portfolio" {:anomaly :failed-optimization})}
                 (pmap (fn [_] (-bets-for-contracts contracts)) (range optimizer-runs)))
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
(def -memoized-get-optimal-bets (memo/lru -get-optimal-bets :lru/threshold 100))

(defn get-optimal-bets [hurdle-return contracts-price-and-prob]
  (let [rounded-contracts (transduce
                            (comp
                              (map #(update % :prob round4))
                              (map #(update % :est-value round4)))
                            conj
                            contracts-price-and-prob)
        result (-memoized-get-optimal-bets hurdle-return rounded-contracts)]
    (when (nil? result)
      ; never cache errors
      (memo/memo-clear! -memoized-get-optimal-bets [hurdle-return contracts-price-and-prob]))
    result))
