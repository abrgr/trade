(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:require [com.adamgberger.predictit.lib.log :as l])
  (:import (de.xypron.jcobyla Calcfc
                              Cobyla
                              CobylaExitStatus))
  (:gen-class))

(defn- odds [pos result]
  (let [{:keys [price trade-type cash?]} pos
        price (if (nil? price)
                nil
                (double price))
        result-yes? (= result :yes)
        result-no? (not result-yes?)
        bet-yes? (or cash? (= trade-type :buy-yes))
        bet-no? (not bet-yes?)]
    (cond
      cash? 0
      (and result-yes? bet-yes?) (/ 1 price)
      (and result-no?  bet-yes?) -1
      (and result-yes? bet-no?)  -1
      (and result-no?  bet-no?)  (/ 1 price))))

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

(defn- exp-return [{:keys [est-value price]}]
  (/ (- est-value price) price))

(defn- -get-optimal-bets [hurdle-return contracts-price-and-prob remaining-attempts]
  (let [filtered-contracts (->> contracts-price-and-prob
                                (filter #(> (exp-return %) hurdle-return))
                                (map
                                 #(assoc
                                   %
                                   :prob
                                   (-> %
                                       :prob
                                       java.math.BigDecimal.
                                       (.round (java.math.MathContext. 4))
                                       double))))
        total-prob (->> filtered-contracts
                        (map :prob)
                        (reduce + 0.0))
        remaining-prob (- 1 total-prob)
        contracts (if (> remaining-prob 0.0)
                    (conj filtered-contracts {:prob remaining-prob :cash? true})
                    filtered-contracts)
        len (count contracts)
          ; constraints are represented as functions that must be non-negative
        non-neg-constraints (map
                             (fn [^Integer i]
                               (fn [x con]
                                 (aset-double con (inc i) (nth x i))))
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
              (doseq [constraint constraints] (constraint x con))
              (* -1 (growth-rate (map #(assoc %1 :wager %2) contracts x)))))
        weights (double-array len (map (fn [_] (rand)) (range len)))
        rho-start 0.5
        rho-end 1.0e-6
        res (Cobyla/findMinimum f len num-constraints weights rho-start rho-end 0 1e5)]
    (if (= CobylaExitStatus/NORMAL res)
      (let [with-weights (->> weights
                              (map
                               #(assoc %1 :weight %2)
                               contracts)
                              (filter (comp not :cash?)) ; remove the cash contract we added
                              (into []))]
        (l/log :info "Optimized portfolio" {:contracts contracts
                                            :orig-contracts contracts-price-and-prob
                                            :result with-weights
                                            :opt-result res})
        with-weights)
      (do (l/log :error "Failed to optimize portfolio" {:contracts contracts
                                                        :orig-contracts contracts-price-and-prob
                                                        :opt-result res})
          (if (> remaining-attempts 0)
                    ; we may get diverging rounding errors for some initial weights, try again with new random weights
            (-get-optimal-bets hurdle-return contracts-price-and-prob (dec remaining-attempts))
            [])))))

(defn get-optimal-bets [hurdle-return contracts-price-and-prob]
  (-get-optimal-bets hurdle-return contracts-price-and-prob 5))