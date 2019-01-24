(ns com.adamgberger.predictit.executors.default-executor-test
  (:require [com.adamgberger.predictit.executors.default-executor :as exec]
            [clojure.test :refer :all]))

(def actuals-from-desired-args
    {:desired-pos [{:contract-id 13964, :target-price 0.9873487542671151, :target-mins 120.0, :trade-type :buy-no, :price 0.79M, :qty 79.0}
                   {:contract-id 13961, :target-price 0.8706304594858997, :target-mins 120.0, :trade-type :buy-no, :price 0.64M, :qty 45.0}
                   {:contract-id 13966, :target-price 0.33841908453469954, :target-mins 120.0, :trade-type :buy-yes, :price 0.12M, :qty 47.0}
                   {:contract-id 13963, :target-price 0.22803119453537668, :target-mins 120.0, :trade-type :buy-yes, :price 0.094M, :qty 18.0}],
     :outstanding-orders-by-contract-id {13960 [],
                                         13962 [],
                                         13964 [{:order-id 23193437, :contract-id 13964, :qty 25, :price 0.75M, :trade-type :buy-no, :created-at "2019-01-22T22:15:41.360Z", :cancellable? true}],
                                         13961 [],
                                         13966 [{:order-id 23193426, :contract-id 13966, :qty 25, :price 0.5M, :trade-type :buy-no, :created-at "2019-01-22T22:13:55.160Z", :cancellable? true}],
                                         13963 [],
                                         13965 []},
     :venue-id :com.adamgberger.predictit.venues.predictit/predictit,
     :current-pos-by-contract-id {13919 {:contract-id 13919, :tradable? false, :side :no, :qty 27, :avg-price-paid 0.9185179999999999456150590049219317734241485595703125M, :orders {:buy 0, :sell 0}},
                                  13914 {:contract-id 13914, :tradable? false, :side :yes, :qty 272, :avg-price-paid 0.8744110000000000493258767164661549031734466552734375M, :orders {:buy 75, :sell 0}},
                                  13964 {:contract-id 13964, :tradable? true, :side :no, :qty 0, :avg-price-paid 0M, :orders {:buy 25, :sell 0}},
                                  13966 {:contract-id 13966, :tradable? true, :side :no, :qty 0, :avg-price-paid 0M, :orders {:buy 25, :sell 0}}},
     :venue-state #:com.adamgberger.predictit.venues.predictit{:predictit
        {:order-books {5191 {13962 {:last-price 0.08, :yes {:buy [{:contract-id 13962, :price 0.09, :qty 20} {:contract-id 13962, :price 0.1, :qty 179} {:contract-id 13962, :price 0.18, :qty 2} {:contract-id 13962, :price 0.2, :qty 10} {:contract-id 13962, :price 0.25, :qty 100} {:contract-id 13962, :price 0.35, :qty 25} {:contract-id 13962, :price 0.5, :qty 1} {:contract-id 13962, :price 0.55, :qty 1} {:contract-id 13962, :price 0.6, :qty 1} {:contract-id 13962, :price 0.65, :qty 1}], :sell [{:contract-id 13962, :price 0.05, :qty 6} {:contract-id 13962, :price 0.03, :qty 50} {:contract-id 13962, :price 0.02, :qty 500} {:contract-id 13962, :price 0.01, :qty 101}]}, :no {:buy
        [{:contract-id 13962, :price 0.95, :qty 6} {:contract-id 13962, :price 0.97, :qty 50} {:contract-id 13962, :price 0.98, :qty 500} {:contract-id 13962, :price 0.99, :qty 101}], :sell [{:contract-id 13962, :price 0.91, :qty 20} {:contract-id 13962, :price 0.9, :qty 179} {:contract-id 13962, :price 0.82, :qty 2} {:contract-id 13962, :price 0.8, :qty 10} {:contract-id 13962, :price 0.75, :qty 100} {:contract-id 13962, :price 0.65, :qty 25} {:contract-id 13962, :price 0.5, :qty 1} {:contract-id 13962, :price 0.45, :qty 1} {:contract-id 13962, :price 0.4, :qty 1} {:contract-id 13962, :price
        0.35, :qty 1}]}}, 13960 {:last-price 0.04, :yes {:buy [{:contract-id 13960, :price 0.05, :qty 200} {:contract-id 13960, :price 0.06, :qty 15} {:contract-id 13960, :price 0.07, :qty 55} {:contract-id 13960, :price 0.08, :qty 26} {:contract-id 13960, :price 0.09, :qty 50} {:contract-id 13960, :price 0.14, :qty 61} {:contract-id 13960, :price 0.15, :qty 268} {:contract-id 13960, :price 0.17, :qty 250} {:contract-id 13960, :price 0.18, :qty 15} {:contract-id 13960, :price 0.21, :qty 4}], :sell [{:contract-id 13960, :price 0.02, :qty 475} {:contract-id 13960, :price 0.01, :qty 104}]}, :no {:buy [{:contract-id 13960, :price 0.98, :qty 475} {:contract-id 13960, :price 0.99, :qty 104}], :sell [{:contract-id 13960, :price 0.95, :qty 200} {:contract-id 13960, :price 0.94, :qty 15} {:contract-id 13960, :price 0.93, :qty 55} {:contract-id 13960, :price 0.92, :qty 26} {:contract-id 13960, :price 0.91, :qty 50} {:contract-id 13960, :price 0.86, :qty 61} {:contract-id 13960, :price 0.85, :qty 268} {:contract-id 13960, :price 0.83, :qty 250} {:contract-id 13960, :price 0.82, :qty 15} {:contract-id 13960, :price 0.79, :qty 4}]}}, 13964 {:last-price 0.23, :yes {:buy [{:contract-id 13964, :price 0.21, :qty 1} {:contract-id 13964, :price 0.23, :qty 16} {:contract-id 13964, :price 0.24, :qty 1} {:contract-id 13964, :price 0.25, :qty 25} {:contract-id 13964, :price 0.26, :qty 100} {:contract-id 13964, :price 0.29, :qty 100} {:contract-id 13964, :price 0.33, :qty 142} {:contract-id 13964, :price 0.34, :qty 25} {:contract-id 13964, :price 0.49, :qty 50} {:contract-id 13964, :price 0.5, :qty 4}], :sell [{:contract-id 13964, :price 0.12, :qty 11} {:contract-id 13964, :price 0.04, :qty 50} {:contract-id 13964, :price 0.03, :qty 50} {:contract-id 13964, :price 0.02, :qty 200} {:contract-id 13964, :price 0.01, :qty 101}]}, :no {:buy [{:contract-id 13964, :price 0.88, :qty 11} {:contract-id 13964, :price 0.96, :qty 50} {:contract-id 13964, :price 0.97, :qty 50} {:contract-id 13964, :price 0.98, :qty 200} {:contract-id 13964, :price 0.99, :qty 101}], :sell [{:contract-id 13964, :price 0.79, :qty 1} {:contract-id 13964, :price 0.77, :qty 16} {:contract-id 13964, :price 0.76, :qty 1} {:contract-id 13964, :price 0.75, :qty 25} {:contract-id 13964, :price 0.74, :qty 100} {:contract-id 13964, :price 0.71, :qty 100} {:contract-id 13964, :price 0.67, :qty 142} {:contract-id 13964, :price 0.66, :qty 25} {:contract-id 13964, :price 0.51, :qty 50} {:contract-id 13964, :price 0.5, :qty 4}]}}, 13965 {:last-price 0.03, :yes {:buy [{:contract-id 13965, :price 0.09, :qty 60} {:contract-id 13965, :price 0.1, :qty 54} {:contract-id 13965, :price 0.11, :qty 50} {:contract-id 13965, :price 0.12, :qty 111} {:contract-id 13965, :price 0.13, :qty 3} {:contract-id 13965, :price 0.17, :qty 1} {:contract-id 13965, :price 0.21, :qty 10} {:contract-id 13965, :price 0.35, :qty 1} {:contract-id 13965, :price 0.4, :qty 84} {:contract-id 13965, :price 0.5, :qty 1}], :sell [{:contract-id 13965, :price 0.02, :qty 100} {:contract-id 13965, :price 0.01, :qty 637}]}, :no {:buy [{:contract-id 13965, :price 0.98, :qty 100} {:contract-id 13965, :price 0.99, :qty 637}], :sell [{:contract-id 13965, :price 0.91, :qty 60} {:contract-id 13965, :price 0.9, :qty 54} {:contract-id 13965, :price 0.89, :qty 50} {:contract-id 13965, :price 0.88, :qty 111} {:contract-id 13965, :price 0.87, :qty 3} {:contract-id 13965, :price 0.83, :qty 1} {:contract-id 13965, :price 0.79, :qty 10} {:contract-id 13965, :price 0.65, :qty 1} {:contract-id 13965, :price 0.6, :qty 84} {:contract-id 13965, :price 0.5, :qty 1}]}}, 13966 {:last-price 0.2, :yes {:buy [{:contract-id 13966, :price 0.39, :qty 1} {:contract-id 13966, :price 0.4, :qty 101} {:contract-id 13966, :price 0.49, :qty 111} {:contract-id 13966, :price 0.5, :qty 26} {:contract-id 13966, :price 0.55, :qty 1} {:contract-id 13966, :price 0.6, :qty 22} {:contract-id 13966, :price 0.65, :qty 1} {:contract-id 13966, :price 0.67, :qty 25} {:contract-id 13966, :price 0.7, :qty 1} {:contract-id 13966, :price 0.75, :qty 1}], :sell [{:contract-id 13966, :price 0.19, :qty 1} {:contract-id 13966, :price 0.18, :qty 9} {:contract-id 13966, :price 0.14, :qty 1} {:contract-id 13966, :price 0.09, :qty 4} {:contract-id 13966, :price 0.06, :qty 30} {:contract-id 13966, :price 0.04, :qty 100} {:contract-id 13966, :price 0.03, :qty 650} {:contract-id 13966, :price 0.01, :qty 201}]}, :no {:buy [{:contract-id 13966, :price 0.81, :qty 1} {:contract-id 13966, :price 0.82, :qty 9} {:contract-id 13966, :price 0.86, :qty 1} {:contract-id 13966, :price 0.91, :qty 4} {:contract-id 13966, :price 0.94, :qty 30} {:contract-id 13966, :price 0.96, :qty 100} {:contract-id 13966, :price 0.97, :qty 650} {:contract-id 13966, :price 0.99, :qty 201}], :sell [{:contract-id 13966, :price 0.61, :qty 1} {:contract-id 13966, :price 0.6, :qty 101} {:contract-id 13966, :price 0.51, :qty 111} {:contract-id 13966, :price 0.5, :qty 26} {:contract-id 13966, :price 0.45, :qty 1} {:contract-id 13966, :price 0.4, :qty 22} {:contract-id 13966, :price 0.35, :qty 1} {:contract-id 13966, :price 0.33, :qty 25} {:contract-id 13966, :price 0.3, :qty 1} {:contract-id 13966, :price 0.25, :qty 1}]}}, 13961 {:last-price 0.25, :yes {:buy [{:contract-id 13961, :price 0.27, :qty 1} {:contract-id 13961, :price 0.29, :qty 1} {:contract-id 13961, :price 0.38, :qty 17} {:contract-id 13961, :price 0.39, :qty 24} {:contract-id 13961, :price 0.4, :qty 50} {:contract-id 13961, :price 0.42, :qty 1} {:contract-id 13961, :price 0.44, :qty 31} {:contract-id 13961, :price 0.45, :qty 221} {:contract-id 13961, :price 0.5, :qty 4} {:contract-id 13961, :price 0.55, :qty 1}], :sell [{:contract-id 13961, :price 0.19, :qty 7} {:contract-id 13961, :price 0.17, :qty 16} {:contract-id 13961, :price 0.16, :qty 170} {:contract-id 13961, :price 0.15, :qty 221} {:contract-id 13961, :price 0.14, :qty 200} {:contract-id 13961, :price 0.12, :qty 1} {:contract-id 13961, :price 0.11, :qty 157} {:contract-id 13961, :price 0.08, :qty 4} {:contract-id 13961, :price 0.05, :qty 250} {:contract-id 13961, :price 0.03, :qty 50}]}, :no {:buy [{:contract-id 13961, :price 0.81, :qty 7} {:contract-id 13961, :price 0.83, :qty 16} {:contract-id 13961, :price 0.84, :qty 170} {:contract-id 13961, :price 0.85, :qty 221} {:contract-id 13961, :price 0.86, :qty 200} {:contract-id 13961, :price 0.88,
        :qty 1} {:contract-id 13961, :price 0.89, :qty 157} {:contract-id 13961, :price 0.92, :qty 4} {:contract-id 13961, :price 0.95, :qty 250} {:contract-id 13961, :price 0.97, :qty 50}], :sell [{:contract-id 13961, :price 0.73, :qty 1} {:contract-id 13961, :price 0.71, :qty 1} {:contract-id 13961, :price 0.62, :qty 17} {:contract-id 13961, :price 0.61, :qty 24} {:contract-id 13961, :price 0.6, :qty 50} {:contract-id 13961, :price 0.58, :qty 1}
        {:contract-id 13961, :price 0.56, :qty 31} {:contract-id 13961, :price 0.55, :qty 221} {:contract-id 13961, :price 0.5, :qty 4} {:contract-id 13961, :price 0.45, :qty 1}]}}, 13963 {:last-price 0.08, :yes {:buy [{:contract-id 13963, :price 0.15, :qty 1} {:contract-id 13963, :price 0.17, :qty 11}
        {:contract-id 13963, :price 0.23, :qty 15} {:contract-id 13963, :price 0.24, :qty 1} {:contract-id 13963, :price 0.37, :qty 2} {:contract-id 13963,
        :price 0.38, :qty 148} {:contract-id 13963, :price 0.5, :qty 1} {:contract-id 13963, :price 0.55, :qty 1} {:contract-id 13963, :price 0.6, :qty 301} {:contract-id 13963, :price 0.65, :qty 1}], :sell [{:contract-id 13963, :price 0.08, :qty 20} {:contract-id 13963, :price 0.07, :qty 101} {:contract-id 13963, :price 0.06, :qty 153} {:contract-id 13963, :price 0.05, :qty 84} {:contract-id 13963, :price 0.04, :qty 105} {:contract-id 13963, :price 0.03, :qty 200} {:contract-id 13963, :price 0.02, :qty 682} {:contract-id 13963, :price 0.01, :qty 876}]}, :no {:buy [{:contract-id 13963, :price
        0.92, :qty 20} {:contract-id 13963, :price 0.93, :qty 101} {:contract-id 13963, :price 0.94, :qty 153} {:contract-id 13963, :price 0.95, :qty 84} {:contract-id 13963, :price 0.96, :qty 105} {:contract-id 13963, :price 0.97, :qty 200} {:contract-id 13963, :price 0.98, :qty 682} {:contract-id 13963, :price 0.99, :qty 876}], :sell [{:contract-id 13963, :price 0.85, :qty 1} {:contract-id 13963, :price 0.83, :qty 11} {:contract-id 13963, :price
        0.77, :qty 15} {:contract-id 13963, :price 0.76, :qty 1} {:contract-id 13963, :price 0.63, :qty 2} {:contract-id 13963, :price 0.62, :qty 148} {:contract-id 13963, :price 0.5, :qty 1} {:contract-id 13963, :price 0.45, :qty 1} {:contract-id 13963, :price 0.4, :qty 301} {:contract-id 13963, :price 0.35, :qty 1}]}}}},
     :orders {5191 {13960 [], 13962 [], 13964 [{:order-id 23193437, :contract-id 13964, :qty 25, :price 0.75M, :trade-type :buy-no,
        :created-at "2019-01-22T22:15:41.360Z", :cancellable? true}], 13961 [], 13966 [{:order-id 23193426, :contract-id 13966, :qty 25, :price 0.5M, :trade-type :buy-no, :created-at "2019-01-22T22:13:55.160Z", :cancellable? true}], 13963 [], 13965 []}}}},
     :mkt-id 5191})

(def actuals
    (set [{:trade-type :buy-no, :contract-id 13964, :qty 54.0, :target-price 0.9873487542671151, :price 0.79M}
          {:trade-type :buy-no, :contract-id 13961, :qty 45.0, :target-price 0.8706304594858997, :price 0.64M}
          {:trade-type :cancel, :target-price 0.33841908453469954, :contract-id 13966, :order-id 23193426}
          {:trade-type :buy-yes, :contract-id 13966, :qty 47.0, :target-price 0.33841908453469954, :price 0.12M}
          {:trade-type :buy-yes, :contract-id 13963, :qty 18.0, :target-price 0.22803119453537668, :price 0.094M}]))

(deftest test-actuals-from-desired
    (testing "adjust-desired-pos-for-actuals"
        (let [{:keys [venue-state venue-id mkt-id desired-pos current-pos-by-contract-id outstanding-orders-by-contract-id]} actuals-from-desired-args
              adj-pos (set
                        (#'exec/adjust-desired-pos-for-actuals
                            venue-state
                            venue-id
                            mkt-id
                            desired-pos
                            current-pos-by-contract-id
                            outstanding-orders-by-contract-id))]
            (is (and (clojure.set/subset? adj-pos actuals)
                     (clojure.set/subset? actuals adj-pos))))))