(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:require [com.adamgberger.predictit.lib.log :as l])
  (:import (de.xypron.jcobyla Calcfc
                              Cobyla
                              CobylaExitStatus))
  (:gen-class))

(defn- odds [pos result]
    (let [{:keys [prob price cash?]} pos
          result-yes? (= result :yes)
          result-no? (not result-yes?)
          bet-yes? (or cash? (>= prob price))
          bet-no? (not bet-yes?)]
        (cond
            cash? 0
            (and result-yes? bet-yes?) (/ 1 price)
            (and result-no?  bet-yes?) -1
            (and result-yes? bet-no?)  -1
            (and result-no?  bet-no?)  (/ 1 (- 1 price)))))

(defn- wager-result [pos result]
    (let [{:keys [wager price]} pos
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
        (Math/log g)))

(defn- growth-rate [contract-positions]
    ; This is adapted from Thorpe's discussion of the Kelly Criterion
    ; http://www.eecs.harvard.edu/cs286r/courses/fall12/papers/Thorpe_KellyCriterion2007.pdf
    (reduce
        (fn [sum {:keys [prob] :as pos}]
            (let [exp-growth (* prob (growth-for-yes-pos contract-positions pos))]
                (+ sum exp-growth)))
        0.0
        contract-positions))

(defn kelly [p b]
    (/
        (-
            (* p (+ b 1))
            1)
        b))

(defn get-optimal-bets [hurdle-return contracts-price-and-prob]
    (let [total-prob (->> contracts-price-and-prob
                            (map :prob)
                            (reduce + 0.0))
          remaining-prob (- 1 total-prob)
          added-cash? (> remaining-prob 0.0)
          contracts (if added-cash?
                        (conj contracts-price-and-prob {:prob remaining-prob :cash? true})
                        contracts-price-and-prob)
          len (count contracts)
          ; constraints are represented as functions that must be non-negative
          non-neg-constraints (map
                                (fn [i]
                                    (fn [x con]
                                        (aset-double con (inc i) (nth x i))))
                                (range len))
          budget-constraint (fn [x con]
                                (aset-double con 0 (-1 (reduce #(+ %1 (Math/abs %2)) 0.0 x))))
          constraints (conj non-neg-constraints budget-constraint)
          num-constraints (count constraints)
          f (reify Calcfc
                (compute [this n m x con]
                    (map #(% x con) constraints)
                    (* -1 (growth-rate (map #(assoc %1 :wager %2) contracts x)))))
          weights (double-array len (repeat (/ 1.0 len)))
          rho-start 0.1
          rho-end 1.0e-6
          res (Cobyla/findMinimum f len num-constraints weights rho-start rho-end 0 10000)]
        (if (= CobylaExitStatus/NORMAL res)
            (->> weights
                 (map
                    #(assoc %1 :weight %2)
                    contracts)
                 (#(if added-cash? (drop-last %) %)) ; remove our filler contract
                 (into []))
            (l/log :error "Failed to optimize portfolio" (merge {:contracts-price-and-prob contracts-price-and-prob})))))