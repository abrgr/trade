(ns com.adamgberger.predictit.inputs.rasmussen
  (:require [clj-http.client :as h]
            [hickory.core :as m]
            [hickory.select :as s]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(defn parse-result [html cb]
    (let [p (-> html
                m/parse
                m/as-hickory)
          date (-> (s/select (s/child (s/class :date))
                             p)
                   first
                   :content
                   first
                   u/parse-human-date-with-day)
          [_ approval] (->> (s/select (s/child (s/find-in-text #"% of Likely U.S. Voters"))
                                            p)
                            first
                            :content
                            first
                            (re-find #" (\d+)% of Likely U\.S\. Voters"))]
        (cb {:val (Integer/parseInt approval)
             :date date})))

(defn get-current [cb]
    (let [date (-> (java.time.LocalDate/now)
                   (.format (java.time.format.DateTimeFormatter/ofPattern "MMMdd"))
                   (.toLowerCase))]
        (h/get
            (str "http://www.rasmussenreports.com/public_content/politics/trump_administration/prez_track_" date)
            {:async? true}
            #(parse-result (:body %) cb)
            #(l/log :error "Failed to get rasmussen report" (l/ex-log-msg %)))))