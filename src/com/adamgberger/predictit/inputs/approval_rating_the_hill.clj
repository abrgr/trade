(ns com.adamgberger.predictit.inputs.approval-rating-the-hill
  (:require [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clj-http.client :as h]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:import (org.apache.tika Tika))
  (:gen-class))

(def id ::id)

(def ^:private approval-question "Do you approve or disapprove of the job Donald Trump is doing as President of the United States?")
(def ^:private net-approve-str "NET APPROVE")

(defn- async-put-once [ch val]
  (async/put!
    ch
    val
    (fn [_] (async/close! ch))))

(defn- async-retry
  ([out-ch pipeline tries]
   (async-retry out-ch pipeline tries tries 0))
  ([out-ch pipeline orig-tries tries skip]
   (let [ch (async/chan)]
     (async/go-loop [i 0]
       (if-let [val (async/<! ch)]
         (if (and (instance? Exception val)
                  (> tries 0))
           (do (l/log :warn "Retrying" (l/ex-log-msg val))
               (async/<! (async/timeout (+ 100 (rand) (* 200 (Math/pow 2 (inc (- orig-tries tries)))))))
               (async-retry out-ch pipeline orig-tries (dec tries) i))
           (do (when (>= i skip) (async/>! out-ch val))
               (recur (inc i))))
         (async/close! out-ch)))
     (pipeline ch))))

(defn- async-mapcat [f in-ch]
  (let [out-ch (async/chan)]
    (async/go-loop []
      (if-let [x (async/<! in-ch)]
        (let [vals (f x)]
          (doseq [val vals]
            (async/>! out-ch val))
          (recur))
        (async/close! out-ch)))
    out-ch))

(defn- async-filter [f in-ch]
  (let [out-ch (async/chan)]
    (async/go-loop []
      (if-let [x (async/<! in-ch)]
        (do
          (when (f x) (async/>! out-ch x))
          (recur))
        (async/close! out-ch)))
    out-ch))

(defn- async-wrap-error [f]
  (fn [val out-ch]
    (if (instance? Exception val)
      (async-put-once out-ch val)
      (f val out-ch))))

(defn- async-thread [f in-ch]
  (let [c (async/chan)]
    (async/pipeline-async 1 c f in-ch)
    c))

(defn- next-expected []
  (u/next-specific-weekday-at (java.time.LocalDate/now) u/ny-time 5 14 0))

(defn- current-from-sheet [stream out-ch]
  (let [tika (Tika.)
        s (.parseToString tika stream)
        q approval-question
        q-idx (string/index-of s q)]
    (if (nil? q-idx)
      (async/close! out-ch)
      (let [q-end-idx (+ 1 q-idx (count q))
            net-approve-idx (string/index-of s net-approve-str q-end-idx)
            net-approve-end-idx (+ 1 net-approve-idx (count net-approve-str))
            net-approve-line-end (string/index-of s "\n" net-approve-end-idx)
            net-approve-pct-line-end (string/index-of s "\n" (inc net-approve-line-end))
            line (subs s (inc net-approve-line-end) net-approve-pct-line-end)
            [_ approval] (re-find #"\s*(\d+)%" line)
            {date :end-date} (u/parse-human-date-range-within s)]
        (async-put-once
         out-ch
         {:val (Integer/parseInt approval)
          :date date
          :next-expected (next-expected)})))))

(defn- parse-google-sheet [csv]
  (let [data (->> csv csv/read-csv (into []))
        question-col 0
        approve-str-col 1
        approve-pct-col 2
        {date :end-date} (u/parse-human-date-range-within (-> data (nth 0) (nth 0)))
        question-line (->> data
                           (keep-indexed #(when (string/includes? (nth %2 question-col) approval-question) %1))
                           first)
        approval-q-line (->> data
                             (keep-indexed #(when (string/includes? (nth %2 approve-str-col) net-approve-str) %1))
                             (filter #(> % question-line))
                             first)
        approval (-> data
                     (nth (inc approval-q-line))
                     (nth approval-pct-col)
                     (.replace "%" "")
                     Integer/parseInt)]
    {:val approval
     :date date
     :next-expected (u/next-expected)}))

(defn- url-stream [url out-ch]
  (l/log :debug "The hill: getting url" {:url url})
  (h/get
   url
   {:async? true
    :as :stream}
   #(do (l/log :debug "The hill: got url" {:url url})
        (async-put-once out-ch (:body %)))
   #(async-put-once out-ch (ex-info "Failed to get URL stream" (merge (l/ex-log-msg %) {:url url}) %))))

(defn test-it [url]
  (let [{stream :body :as stuff} (h/get url {:as :stream})
        _ (println stuff)
        ;_ (println "HERE" (.read (java.io.InputStreamReader. stream)) "YO")
        ;stream (java.io.ByteArrayInputStream. (.getBytes body "UTF-8"))
        tika (Tika.)
        s (.parseToString tika stream)
        q approval-question
        q-idx (string/index-of s q)]
    (if (nil? q-idx)
      {:nope "nope"}
      (let [q-end-idx (+ 1 q-idx (count q))
            net-approve-str "NET APPROVE"
            net-approve-idx (string/index-of s net-approve-str q-end-idx)
            net-approve-end-idx (+ 1 net-approve-idx (count net-approve-str))
            net-approve-line-end (string/index-of s "\n" net-approve-end-idx)
            net-approve-pct-line-end (string/index-of s "\n" (inc net-approve-line-end))
            line (subs s (inc net-approve-line-end) net-approve-pct-line-end)
            [_ approval] (re-find #"\s*(\d+)%" line)
            {date :end-date} (u/parse-human-date-range-within s)]
        {:val (Integer/parseInt approval)
         :date date
         :next-expected (u/next-specific-weekday-at (java.time.LocalDate/now) u/ny-time 5 14 0)}))))

(defn- article-html-to-sheet-url [html out-ch]
  (let [regex #"<iframe src=\"(https://onedrive.live.com/embed?[^\"]+)\"|<iframe src=\"https://docs.google.com/spreadsheets/[^\"?]+"
        [_ sheet-url] (re-find regex html)]
    (if (some? sheet-url)
      (async-put-once
       out-ch
       (-> sheet-url
           (string/replace "/embed?" "/download?")
           (string/replace "&amp;" "&")))
      (async/close! out-ch))))

(defn- article-html [url out-ch]
  (h/get
   url
   {:async? true}
   #(async-put-once out-ch (:body %))
   #(async-put-once out-ch (ex-info "Failed to get article html" {:url url} %))))

(defn- get-articles [html]
  (let [regex #"\"(/hilltv/what-americas-thinking/[^\"]+)\""
        matches (re-seq regex html)
        article-urls (->> matches
                          (map second)
                          distinct
                          (map (partial str "https://thehill.com")))]
    article-urls))

(defn- async-memoize [f]
  (let [mem (atom (cache/lru-cache-factory {} :threshold 10))]
    (fn [& args]
      (let [out-ch (last args)
            args' (butlast args)
            mem' @mem]
        (if-let [e (cache/has? mem' args')]
          (async-put-once out-ch (get (cache/hit mem' args') args'))
          (let [c (async/chan)]
            (async/go (if-let [ret (async/<! c)]
                        (do (swap! mem cache/miss args' ret)
                            (async-put-once out-ch ret))
                        (async/close! out-ch)))
            (apply f (concat args' [c]))))))))

(defn- onedrive-sheet-url-to-approval-val [url out-ch]
  (async-retry
   out-ch
   (fn [ch]
     (->> (async/to-chan [url])
          (async-thread (async-wrap-error url-stream))
          (async-thread (async-wrap-error current-from-sheet))
          (#(async/pipe % ch))))
   0))

(defn- google-sheet-url-to-approval-val [url out-ch]
  (if-let [csv-url (u/get-google-csv-url url)]
    (h/get
       csv-url
       {:async? true}
       #(async-put-once out-ch (parse-google-sheet (:body %)))
       #(do (l/log :error "Failed to get harris interactive spreadsheet" (merge (l/ex-log-msg %) {:url csv-url})
            (async/close! out-ch))
    (async/close! out-ch)))

(defn- -sheet-url-to-approval-val [url out-ch]
  (if (string/includes? url "onedrive.live.com")
    (onedrive-sheet-url-to-approval-val url out-ch)
    (google-sheet-url-to-approval-val url out-ch)))

(def ^:private sheet-url-to-approval-val (async-memoize -sheet-url-to-approval-val))

(defn get-current [cb]
  (let [html-chan (async/chan)
        err-cb #(do (l/log :error "Failed to get the hill poll" (l/ex-log-msg %))
                    (cb %))]
    (h/get
     "https://thehill.com/hilltv/america"
     {:async? true}
     #(async/go (async/>! html-chan (:body %))
                (async/close! html-chan))
     err-cb)
    (->> html-chan
         (async-mapcat get-articles)
         (async-thread (async-wrap-error article-html))
         (async-thread (async-wrap-error article-html-to-sheet-url))
         (async-thread (async-wrap-error sheet-url-to-approval-val))
         (u/tap-timeout 5000 #(cb nil))
         (async/take 1)
         (async-thread (async-wrap-error #(do (cb %1) (async/close! %2))))
         (async-thread #(do (l/log :error "Error in the hill approval rating" (l/ex-log-msg %1))
                            (async/close! %2)
                            (cb %))))))
