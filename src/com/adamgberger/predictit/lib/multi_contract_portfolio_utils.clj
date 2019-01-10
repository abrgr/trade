(ns com.adamgberger.predictit.lib.multi-contract-portfolio-utils
  (:import (org.apache.commons.math3.optim InitialGuess
                                           OptimizationData
                                           MaxEval)
           (org.apache.commons.math3.optim.nonlinear.scalar ObjectiveFunction
                                                            GoalType
                                                            MultivariateFunctionMappingAdapter)
           (org.apache.commons.math3.optim.nonlinear.scalar.noderiv PowellOptimizer)
           (org.apache.commons.math3.analysis MultivariateFunction))
  (:gen-class))

(defn expected-portfolio-value
    "Calculates the expected value of a portfolio of the given contracts.
     Requires that (= (->> contract-positions (map :prob) +) 1)."
    [contract-positions]
    (let [contract-value (fn [yn contract]
                            (let [price (:price contract)
                                  odds (/ 1 price)
                                  wager (:wager contract)
                                  winner? (or
                                            (and (< wager 0)
                                                 (= yn :no))
                                            (and (>= wager 0)
                                                 (= yn :yes)))]
                                (if winner?
                                    (* wager odds)
                                    (* -1 wager))))
           no-sum (->> contract-positions
                       (map (partial contract-value :no))
                       (reduce +))
           total-wagered (reduce + (map :wager contract-positions))]
        (->> contract-positions
             (map
                (fn [pos]
                    (let [p (:prob pos)
                          xn (+
                                (contract-value :yes pos)
                                no-sum
                                (* -1 (contract-value :no pos)))
                          b (if (< (java.lang.Math/abs total-wagered) 0.000001)
                                0.0
                                (/ xn total-wagered))]
                    (* p (java.lang.Math/log (+ 1 b))))))
             (reduce +)
             (#(do (println %) %)))))

(defn odds [pos result]
    (cond
        (and (= result :yes) (>= (:wager pos) 0)) (/ 1 (:price pos))
        (and (= result :no)  (>= (:wager pos) 0)) -1
        (and (= result :yes) (< (:wager pos)  0)) -1
        (and (= result :no)  (< (:wager pos)  0)) (/ 1 (- 1 (:price pos)))))

; TODO: deal with no bets correctly
(defn growth-rate [contract-positions]
    (->> contract-positions
         (reduce
            (fn [sum pos]
                (+ sum
                   (* (:prob pos)
                      (java.lang.Math/log (+ 1 (* (odds pos :yes) (:wager pos)))))
                   (* (- 1 (:prob pos))
                      (java.lang.Math/log (+ 1 (* (odds pos :no) (:wager pos)))))))
            0.0)))

(defn get-optimal-bets [contracts-price-and-prob]
    (let [f (reify MultivariateFunction
                (value [this point]
                    (growth-rate
                        (map #(assoc %1 :wager %2) contracts-price-and-prob point))))
          min-wager -1
          max-wager 1
          len (count contracts-price-and-prob)
          obj (MultivariateFunctionMappingAdapter.
                f
                (double-array len (repeat min-wager))
                (double-array len (repeat max-wager)))
          optimizer (PowellOptimizer. 1e-13 1e-40)
          opt (.optimize
                optimizer
                (into-array
                    OptimizationData
                    [(MaxEval. 1000000)
                     (ObjectiveFunction. obj)
                     GoalType/MAXIMIZE
                     (InitialGuess. (double-array len (repeat 0)))]))]
            (->> opt
                 .getPoint
                 (.unboundedToBounded obj)
                 (into []))))