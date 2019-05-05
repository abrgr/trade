(ns com.adamgberger.predictit.inputs.yougov-weekly-registered
  (:require [clj-http.client :as h]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(def id ::id)

(defn parse-registered-approval-from-econtabs
  "Parses a document like https://d25d2506sfb94s.cloudfront.net/cumulus_uploads/document/f0al8raalq/econTabReport.pdf
     to get the likely voters job approval for president trump as used in the RCP aggregate."
  [stream]
  (let [tika (org.apache.tika.Tika.)
        _ (.setMaxStringLength tika 1000000)
        s (.parseToString tika stream)
        intro (.substring s 0 500)
        dates (u/parse-human-date-range-within intro)
        job-approval-idx (clojure.string/last-index-of s "Trump Job Approval")
        question-idx (clojure.string/index-of s "Do you approve or disapprove of the way Donald Trump is handling his job as President?" job-approval-idx)
        header-1-idx (clojure.string/index-of s "Registered voters" question-idx)
        header-2-idx (clojure.string/index-of s "Total Registered" header-1-idx)
        strongly-approve-str "Strongly approve"
        after-strongly-approve-idx (+ (count strongly-approve-str)
                                      (clojure.string/index-of s strongly-approve-str header-2-idx))
        total-strongly-approve-% (inc (clojure.string/index-of s "%" after-strongly-approve-idx))
        registered-strongly-approve-% (clojure.string/index-of s "%" total-strongly-approve-%)
        somewhat-approve-str "Somewhat approve"
        after-somewhat-approve-idx (+ (count somewhat-approve-str)
                                      (clojure.string/index-of s somewhat-approve-str after-strongly-approve-idx))
        total-somewhat-approve-% (inc (clojure.string/index-of s "%" after-somewhat-approve-idx))
        registered-somewhat-approve-% (clojure.string/index-of s "%" total-somewhat-approve-%)
        strongly-approve (-> s
                             (.substring total-strongly-approve-% registered-strongly-approve-%)
                             .trim
                             Integer/parseInt)
        somewhat-approve (-> s
                             (.substring total-somewhat-approve-% registered-somewhat-approve-%)
                             .trim
                             Integer/parseInt)
        {:keys [end-date]} dates
        next-expected (-> end-date
                          (.atStartOfDay u/ny-time)
                          (.plus 1 java.time.temporal.ChronoUnit/WEEKS)
                          (.withHour 16)
                          (.withMinute 0))]
        ; TODO: validate
    {:val (+ strongly-approve somewhat-approve)
     :date end-date
     :next-expected next-expected}))

(defn extract-link [html]
  (let [pat #"https://[^.]+.cloudfront.net/cumulus_uploads/document/[^/]+/econTabReport.pdf"]
    (re-find pat html)))

(def most-recent-result (atom nil))

(defn check-search-results [html cb]
  (let [pdf-link (extract-link html)
        cached @most-recent-result]
    (if (= pdf-link (:link cached))
      (cb (:res cached))
      (h/get
       pdf-link
       {:async? true
        :as :stream}
       #(let [val (parse-registered-approval-from-econtabs (:body %))]
          (swap! most-recent-result (constantly {:res val :link pdf-link}))
          (cb val))
       #(l/log :error "Failed to get yougov-weekly-registered pdf" (merge {:url pdf-link} (l/ex-log-msg %)))))))

(defn get-current [cb]
  (h/get
   "https://today.yougov.com/ratings/overview(popup:search/%22economist%20tables%22;type=surveys;period=week)"
   {:async? true}
   #(check-search-results (:body %) cb)
   cb))
