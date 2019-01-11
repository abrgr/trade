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

(defn- odds [pos result]
    (cond
        (and (= result :yes) (>= (:wager pos) 0)) (/ 1 (:price pos))
        (and (= result :no)  (>= (:wager pos) 0)) -1
        (and (= result :yes) (< (:wager pos)  0)) 1
        (and (= result :no)  (< (:wager pos)  0)) (/ 1 (- 1 (:price pos)))))

(defn- wager-result [pos result]
    (let [{:keys [wager prob price]} pos
          b (odds pos result)
          bet-no? (< prob price) ; TODO: would prefer to generate endogenously from the optimization but can't quite make that work
          result-prob (if (= result :yes)
                          prob
                          (- 1 prob))]
        (if bet-no?
            (wager-result
                {:prob (- 1 prob)
                 :wager (* -1 wager)
                 :price (- 1 price)}
                (if (= result :yes) :no :yes))
            (* result-prob
               (java.lang.Math/log (+ 1 (* b wager )))))))

(defn- growth-for-pos [pos]
    (let [yes-prob (:prob pos)
          no-prob (- 1 yes-prob)]
        (+ (wager-result pos :yes)
           (wager-result pos :no))))

(defn- growth-rate [contract-positions]
    ; This is adapted from Thorpe's discussion of the Kelly Criterion
    ; http://www.eecs.harvard.edu/cs286r/courses/fall12/papers/Thorpe_KellyCriterion2007.pdf
    (reduce
        (fn [sum pos] (+ sum (growth-for-pos pos)))
        0.0
        contract-positions))

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