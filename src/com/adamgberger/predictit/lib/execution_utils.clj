(ns com.adamgberger.predictit.lib.execution-utils
    (:require [com.adamgberger.predictit.lib.log :as l])
    (:gen-class))

(def keypath-by-trade-type
    {:buy-yes [:yes :buy]
     :buy-no [:no :buy]
     :sell-yes [:yes :sell]
     :sell-no [:no :sell]})

(defn- other-side-keypath [[yn buy-sell]]
    (case buy-sell
        :buy [yn :sell]
        :sell [yn :buy]))

(defn- find-price-for-mins [^Double immediate-price ^Double last ^Double cur-best ^Integer mins]
    (let [pts (org.apache.commons.math3.fitting.WeightedObservedPoints.)
          fitter (org.apache.commons.math3.fitting.PolynomialCurveFitter/create 2)
          epsilon (double 0.01)
          lower-bound (min cur-best immediate-price)
          upper-bound (max cur-best immediate-price)
          mid (/ (+ cur-best immediate-price) 2)
          last-price (when (some? last)
                        (max lower-bound (min last upper-bound)))
          xform-y (fn ^Double [^Double y] (Math/log (+ (Math/abs (double (- y immediate-price))) epsilon)))
          xform-x (fn ^Double [^Integer x] (Math/log (+ x epsilon)))
          dir (if (< cur-best immediate-price) - +)
          un-xform-y (fn [y] (dir immediate-price (Math/exp y)))]
        (.add pts 1.0 (xform-x 0) (xform-y immediate-price))
        (when (some? last-price)
            (.add pts 2.0 (xform-x 10) (xform-y last-price)))
        (.add pts 0.5 (xform-x 30) (xform-y mid))
        (.add pts 1.0 (xform-x 100) (xform-y cur-best))
        (let [f (org.apache.commons.math3.analysis.polynomials.PolynomialFunction. (.fit fitter (.toList pts)))
              x (double (xform-x mins))]
            (un-xform-y (.value f x)))))

(defn- best-price [orders] ; we assume orders is sorted as we want it
    (or (some->> orders
                 (filter #(> (:qty %) 10))
                 first
                 :price)
        (->> orders first :price)))

(defn- mid [best-bid best-ask]
    (/ (+ (or best-ask 0.99) (or best-bid 0.01)) 2))

(defn determine-trade-type [est-value order-book]
    ; TODO: be smarter
    (let [{:keys [last-price]} order-book
          best-yes-ask (->> order-book :yes :buy best-price)
          best-yes-bid (->> order-book :yes :sell best-price)
          best-no-ask (->> order-book :no :buy best-price)
          best-no-bid (->> order-book :no :sell best-price)
          yes-mid (mid best-yes-bid best-yes-ask)
          no-mid (mid best-no-bid best-no-ask)]
        (cond
            (> est-value yes-mid) :buy-yes
            (> (- 1 est-value) no-mid) :buy-no
            :else nil)))

(defn get-likely-fill [mins est-value order-book]
    (let [{:keys [last-price]} order-book
          trade-type (determine-trade-type est-value order-book)]
        (if (and (some? trade-type) (some? order-book))
            (let [keypath (trade-type keypath-by-trade-type)
                  opp-keypath (other-side-keypath keypath)
                  our-side-orders (get-in order-book keypath)
                  opp-side-orders (get-in order-book opp-keypath)
                  our-side-est-value (if (= trade-type :buy-no)
                                         (- 1 est-value)
                                         est-value)
                  immediate-price (or (best-price opp-side-orders)
                                      (min 0.99 (* our-side-est-value 1.6)))
                  last-price (if (and (= trade-type :buy-no)
                                      (some? last-price))
                                 (- 1 last-price)
                                 last-price)
                  cur-best (or (best-price our-side-orders)
                               (max (* our-side-est-value 0.6) (or last-price 0.01)))
                  likely-price (find-price-for-mins immediate-price last-price cur-best mins)
                  usable-price (min
                                    our-side-est-value
                                    (max
                                        0.01
                                        (cond
                                            (= trade-type :buy-yes) (max likely-price (* 0.2 our-side-est-value))
                                            (= trade-type :buy-no) (max likely-price (* 0.2 our-side-est-value))
                                            :else nil)))]
                (l/log :info "Calculated likely fill" {:likely-price likely-price
                                                       :usable-price usable-price 
                                                       :mins mins
                                                       :est-value est-value
                                                       :last-price last-price
                                                       :trade-type trade-type
                                                       :immediate-price immediate-price
                                                       :cur-best cur-best})
                {:price usable-price
                 :est-value our-side-est-value
                 :trade-type trade-type})
            nil)))