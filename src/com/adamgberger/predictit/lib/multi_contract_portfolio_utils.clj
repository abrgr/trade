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

(defn- all-events [contracts]
  (let [sum-prob (->> contracts (map :prob-yes) (apply +))
        extra-prob (- 1.0 sum-prob)
        std-events (map-indexed
                     (fn [idx contract]
                       {:idx idx
                        :contract-idx idx
                        :contract contract})
                     contracts)]
    (if (> extra-prob 0.0001)
      (concat
        std-events
        [{:idx (count contracts)
          :contract-idx -1
          :contract {:prob-yes extra-prob :special :all-lose}}])
      std-events)))

(defn- cash-contracts [contracts]
  [{:idx 0
    :contract-idx -1
    :contract :cash}])

(defn- yes-contracts [contracts]
  (let [last-cash (->> contracts cash-contracts last :idx)]
    (map-indexed
      (fn [idx contract]
        {:idx (+ last-cash 1 idx)
         :contract-idx idx
         :contract contract})
      contracts)))

(defn- no-contracts [contracts]
  (let [last-yes (->> contracts yes-contracts last :idx)]
    (map-indexed
      (fn [idx contract]
        {:idx (+ last-yes 1 idx)
         :contract-idx idx
         :contract contract})
      contracts)))

(defn- all-contracts [contracts]
  (concat (cash-contracts contracts)
          (yes-contracts contracts)
          (no-contracts contracts)))

(defn- events-by-contracts [contracts f]
  (let [events (all-events contracts)
        cash-bets (cash-contracts contracts)
        yes-bets (yes-contracts contracts)
        no-bets (no-contracts contracts)
        e-by-c (make-array Double/TYPE (count events) (apply + (map count [cash-bets yes-bets no-bets])))]
    (doseq [{event :idx} events]
      (doseq [{:keys [idx contract]} cash-bets]
        (aset e-by-c event idx (f event contract :yes :winner)))
      (doseq [{:keys [idx contract-idx contract]} yes-bets]
        (aset e-by-c event idx (f event contract :yes (if (and (= contract-idx event) (not= (:special contract) :all-lose)) :winner :loser))))
      (doseq [{:keys [idx contract-idx contract]} no-bets]
        (aset e-by-c event idx (f event contract :no (if (or (= contract-idx event) (= (:special contract) :all-lose)) :loser :winner)))))
    e-by-c))

(defn- winner-matrix [contracts]
  (events-by-contracts
    contracts
    (fn [_ _ _ wl]
      (if (= wl :winner) 1.0 0.0001))))

(defn- prob-vector [contracts]
  (double-array (map #(-> % :contract :prob-yes) (all-events contracts))))

(defn- odds-with-commission [price]
  (let [commission-rate 0.1
        gross-win (- 1.0 price)]
    (/ (- 1.0 (* commission-rate gross-win)) price)))

(defn- odds-matrix [contracts]
  (events-by-contracts
    contracts
    (fn [e c yn wl]
      (cond
        (= c :cash) 1.0 ; cash returns 1x
        (= wl :loser) 0.0001 ; losers return 0x
        :else (odds-with-commission (get c ({:yes :price-yes :no :price-no} yn))))))) ; winners return their odds

(defn- objective-fn [contracts]
  (->> contracts
       all-events
       (map
         (fn [{event-idx :idx}]
           ["p(" event-idx ") * ln("
            (->> contracts
                 all-contracts
                 (map
                   (fn [{bet-idx :idx}]
                     (str "M(" event-idx ", " bet-idx ") * x(" bet-idx ") * o(" event-idx ", " bet-idx ")")))
                 (interpose " + "))
            ")"]))
       (interpose " + ")
       flatten
       (apply str)))

(defn- constraints [contracts]
  (let [to-var (fn [{:keys [idx]}] (str "x(" idx ")"))
        n-contracts (count contracts)
        vars (->> contracts all-contracts (map to-var))
        yes-vars (->> contracts yes-contracts (map to-var))
        no-vars (->> contracts no-contracts (map to-var))]
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

(defn -bets-for-contracts [hurdle-return contracts prev-contracts]
  (try
    (let [op (OptimizationProblem.)]
      (.setInputParameter op "M" (DoubleMatrixND. (winner-matrix contracts) "dense"))
      (.setInputParameter op "p" (DoubleMatrixND. (prob-vector contracts) "row"))
      (.setInputParameter op "o" (DoubleMatrixND. (odds-matrix contracts) "dense"))
      (.addDecisionVariable op "x" false (int-array 1 [(-> contracts all-contracts count)]) 0.0001 1.0)
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
                                     (merge % {:weight 0.0} (select-keys c [:price :price-yes :price-no :trade-type :weight])))))]
        (if ok
            (let [growth-rate (-> (.getObjectiveFunction op)
                                  (.evaluate (object-array ["x" (.getPrimalSolution op "x")]))
                                  (.get 0)
                                  Math/exp)
                  prev-growth-rate (calc-prev-growth-rate op x-mat prev-contracts)]
              ; TODO: do we want to ensure that fill-mins is the same in prev and new contracts?
              (if (or (nil? prev-growth-rate)
                      (> growth-rate (+ hurdle-return prev-growth-rate))) ; no changes unless we're improving by hurdle-return
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
    (let [{:keys [bets growth-rate prev-growth-rate error]} (-bets-for-contracts hurdle-return contracts prev-contracts)]
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
