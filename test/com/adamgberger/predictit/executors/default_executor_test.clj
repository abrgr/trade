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
     :current-pos-by-contract-id {13919 {:contract-id 13919, :market-id 2, :tradable? false, :side :no, :qty 27, :avg-price-paid 0.9185179999999999456150590049219317734241485595703125M, :orders {:buy 0, :sell 0}},
                                  13914 {:contract-id 13914, :market-id 2, :tradable? false, :side :yes, :qty 272, :avg-price-paid 0.8744110000000000493258767164661549031734466552734375M, :orders {:buy 75, :sell 0}},
                                  13964 {:contract-id 13964, :market-id 5191, :tradable? true, :side :no, :qty 0, :avg-price-paid 0M, :orders {:buy 25, :sell 0}},
                                  12345 {:contract-id 12345, :market-id 5191, :tradable? true, :side :no, :qty 20, :avg-price-paid 0.10M, :orders {:buy 0, :sell 0}},
                                  13966 {:contract-id 13966, :market-id 5191, :tradable? true, :side :no, :qty 0, :avg-price-paid 0M, :orders {:buy 25, :sell 0}}},
     :venue-state #:com.adamgberger.predictit.venues.predictit{:predictit
        {:order-books {5191 {12345 {:last-price 0.08,
                                    :yes {:buy [{:contract-id 12345, :price 0.35, :qty 25}],
                                          :sell [{:contract-id 12345, :price 0.05, :qty 6}]},
                                    :no {:buy [{:contract-id 12345, :price 0.95, :qty 6}],
                                         :sell [{:contract-id 12345, :price 0.91, :qty 20}]}}}}
         :orders {5191 {13960 [],
                        13962 [],
                        13964 [{:order-id 23193437, :contract-id 13964, :qty 25, :price 0.75M, :trade-type :buy-no,
        :created-at "2019-01-22T22:15:41.360Z", :cancellable? true}],
                        13961 [],
                        13966 [{:order-id 23193426, :contract-id 13966, :qty 25, :price 0.5M, :trade-type :buy-no, :created-at "2019-01-22T22:13:55.160Z", :cancellable? true}],
                        13963 [],
                        13965 []}}}},
     :mkt-id 5191})

(def actuals
    (set [{:trade-type :buy-no, :contract-id 13964, :qty 54.0, :target-price 0.9873487542671151, :price 0.79M}
          {:trade-type :buy-no, :contract-id 13961, :qty 45.0, :target-price 0.8706304594858997, :price 0.64M}
          {:trade-type :cancel, :target-price 0.33841908453469954, :contract-id 13966, :order-id 23193426}
          {:trade-type :buy-yes, :contract-id 13966, :qty 47.0, :target-price 0.33841908453469954, :price 0.12M}
          {:trade-type :buy-yes, :contract-id 13963, :qty 18.0, :target-price 0.22803119453537668, :price 0.094M}
          {:trade-type :sell-no, :contract-id 12345, :qty 20, :target-price 0.10M, :price 0.10M}])) ; TODO: types are different

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
            (println adj-pos)
            (is (and (clojure.set/subset? adj-pos actuals)
                     (clojure.set/subset? actuals adj-pos))))))