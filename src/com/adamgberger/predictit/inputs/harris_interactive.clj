(ns com.adamgberger.predictit.inputs.harris-interactive
  (:require [clojure.data.csv :as csv]
            [clj-http.client :as h]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l]
            [hickory.core :as html]
            [hickory.select :as sel])
  (:gen-class))

(def id ::id)

(defn- parse-spreadsheet [csv cb]
    (let [data (csv/read-csv csv)
          date (u/parse-human-date-range-within (-> data first first))
          line-pairs (partition 2 1 data) ; [[1st-line 2nd-line] [2nd-line 3rd line] ...]
          approval (some->> line-pairs
                            (filter (fn [[l1 _]] (= "NET APPROVE" (nth l1 1))))
                            first
                            second ; we want the line after NET APPROVE
                            (filter #(> (count %) 0)) ; non-empty cells
                            first ; first non-empty cell is our approval %
                            (#(.replace % "%" ""))
                            Integer/parseInt)]
        (cb {:val approval
             :date (:end-date date)})))

(defn- check-google-sheet [html cb]
    (let [regex #"https://docs.google.com/spreadsheets/[^\"?]+"
          url (some-> (re-find regex html)
                      (.replace "/pubhtml" "/pub?single=true&output=csv"))]
        (if (nil? url)
            (l/log :error "Failed to find spreadsheet url in harris interactive main page")
            (h/get
                url
                {:async? true}
                #(parse-spreadsheet (:body %) cb)
                #(l/log :error "Failed to get harris interactive spreadsheet" (merge (l/ex-log-msg %) {:url url}))))))

(defn get-current [cb]
  (h/get
    "https://scottrasmussen.com/presidents-job-approval-daily/"
    {:async? true}
    #(check-google-sheet (:body %) cb)
    #(l/log :error "Failed to get harris interactive main page" (l/ex-log-msg %))))