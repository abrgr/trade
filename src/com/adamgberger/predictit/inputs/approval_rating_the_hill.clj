(ns com.adamgberger.predictit.inputs.approval-rating-the-hill
  (:require [clojure.string :as string]
            [clj-http.client :as h]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:import (org.apache.tika Tika))
  (:gen-class))

(def id ::id)

(defn- current-from-sheet [stream cb err-cb]
  (let [tika (Tika.)
        s (.parseToString tika stream)
        q "Do you approve or disapprove of the job Donald Trump is doing as President of the United States?"
        q-idx (string/index-of s q)]
    (if (nil? q-idx)
        (cb nil)
        (let [q-end-idx (+ 1 q-idx (count q))
              net-approve-str "NET APPROVE"
              net-approve-idx (string/index-of s net-approve-str q-end-idx)
              net-approve-end-idx (+ 1 net-approve-idx (count net-approve-str))
              net-approve-line-end (string/index-of s "\n" net-approve-end-idx)
              net-approve-pct-line-end (string/index-of s "\n" (inc net-approve-line-end))
              line (subs s (inc net-approve-line-end) net-approve-pct-line-end)
              [_ approval] (re-find #"\s*(\d+)%" line)
              {date :end-date} (u/parse-human-date-range-within s)]
          (cb {:val (Integer/parseInt approval)
              :date date
              :next-expected (u/next-specific-weekday-at (java.time.LocalDate/now) u/ny-time 5 14 0)})))))

(defn- current-from-article-html [html cb err-cb]
  (let [regex #"<iframe src=\"(https://onedrive.live.com/embed?[^\"]+)\""
        [_ sheet-url] (re-find regex html)]
    (when (some? sheet-url)
      (h/get
        (-> sheet-url
            (string/replace "/embed?" "/download?")
            (string/replace "&amp;" "&"))
        {:async? true
         :as :stream}
        #(current-from-sheet (:body %) cb err-cb)
        err-cb))))

(defn- current-from-article [url cb err-cb]
  (h/get
    (str "https://thehill.com" url)
    {:async? true}
    #(current-from-article-html (:body %) cb err-cb)
    err-cb))

(defn- current-from-articles [html cb err-cb]
  (let [regex #"\"(/hilltv/what-americas-thinking/[^\"]+)\""
        matches (re-seq regex html)]
    (doseq [[_ url] (take 10 matches)]
      (current-from-article
        url
        #(when (some? %) (cb %))
        err-cb))))


(defn get-current [cb]
  (let [err-cb #(l/log :error "Failed to get the hill poll" (l/ex-log-msg %))]
    (h/get
      "https://thehill.com/hilltv/america"
      {:async? true}
      #(current-from-articles (:body %) cb err-cb)
      err-cb)))