(ns com.adamgberger.predictit.lib.execution-utils-test
  (:require [com.adamgberger.predictit.lib.execution-utils :as exec]
            [clojure.test :refer :all]))

(deftest adjust-order-book-for-orders
  (testing "adjust-order-book-for-orders"
    (let [order-book {:last-price 0.53,
                      :yes {:buy [{:contract-id 14269, :price 0.66M, :qty 29} {:contract-id 14269, :price 0.67M, :qty 20}],
                            :sell [{:contract-id 14269, :price 0.53M, :qty 3} {:contract-id 14269, :price 0.52M, :qty 40}]},
                      :no {:buy [{:contract-id 14269, :price 0.47M, :qty 3} {:contract-id 14269, :price 0.48M, :qty 40}],
                           :sell [{:contract-id 14269, :price 0.34M, :qty 29} {:contract-id 14269, :price 0.33M, :qty 20}]}}
          exp {:last-price 0.53,
               :yes {:buy [{:contract-id 14269, :price 0.66M, :qty 29} {:contract-id 14269, :price 0.67M, :qty 20}],
                     :sell [{:contract-id 14269, :price 0.53M, :qty 3} {:contract-id 14269, :price 0.52M, :qty 35}]},
               :no {:buy [{:contract-id 14269, :price 0.47M, :qty 3} {:contract-id 14269, :price 0.48M, :qty 35}],
                    :sell [{:contract-id 14269, :price 0.34M, :qty 29} {:contract-id 14269, :price 0.33M, :qty 20}]}}
          orders [{:order-id 23549208, :contract-id 14269, :qty 5, :price 0.52M, :trade-type :buy-yes, :created-at (java.time.Instant/now), :cancellable? true}]
          adj (#'exec/adjust-order-book-for-orders order-book orders)]
      (is (= adj exp)))))