(ns com.adamgberger.predictit.inputs.yougov-daily-total
  (:require [clj-http.client :as h]
            [com.adamgberger.predictit.lib.utils :as u]
            [com.adamgberger.predictit.lib.log :as l])
  (:gen-class))

(defn parse-total-approval-from-trumptweets
    "Parses a document like https://d25d2506sfb94s.cloudfront.net/cumulus_uploads/document/5wkq01w96u/tabs_Trump_Tweets_20190106.pdf
     to get the total job approval for president trump as used in the 538 aggregate."
    [stream]
    (let [tika (org.apache.tika.Tika.)
          s (.parseToString tika stream)
          job-approval-idx (clojure.string/index-of s "President Trump Job Approval")
          question-idx (clojure.string/index-of s "Do you approve or disapprove of the way Donald Trump is handling his job as President?" job-approval-idx)
          header-idx (clojure.string/index-of s "Registered voters Gender Age" question-idx)
          strongly-approve-str "Strongly approve"
          after-strongly-approve-idx (+ (count strongly-approve-str)
                                        (clojure.string/index-of s strongly-approve-str question-idx))
          strongly-approve-% (clojure.string/index-of s "%" after-strongly-approve-idx)
          somewhat-approve-str "Somewhat approve"
          after-somewhat-approve-idx (+ (count somewhat-approve-str)
                                        (clojure.string/index-of s somewhat-approve-str after-strongly-approve-idx))
          somewhat-approve-% (clojure.string/index-of s "%" after-somewhat-approve-idx)
          strongly-approve (-> s
                               (.substring after-strongly-approve-idx strongly-approve-%)
                               .trim
                               Integer/parseInt)
          somewhat-approve (-> s
                               (.substring after-somewhat-approve-idx somewhat-approve-%)
                               .trim
                               Integer/parseInt)]
        ; TODO: validate that somewhat-approve-idx immediately follows strongly-approve-idx i.f. header-idx i.f. question-idx i.f. job-approval-idx
        (+ strongly-approve somewhat-approve)))

(defn extract-link [html]
  (let [pat #"https://[^.]+.cloudfront.net/cumulus_uploads/document/[^/]+/tabs_Trump_Tweets_\d{8}.pdf"]
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
        #(let [val (parse-total-approval-from-trumptweets (:body %))]
            (swap! most-recent-result (constantly {:res val :link pdf-link}))
            (cb val))
        #(l/log :error "Failed to get yougov-daily-total pdf" (merge {:url pdf-link} (l/ex-log-msg %)))))))


(defn get-current [cb]
  (h/get
    "https://today.yougov.com/ratings/overview(popup:search/%22president%20trump%20daily%20job%20approval%22;type=surveys;period=hours)"
    {:async? true}
    #(check-search-results (:body %) cb)
    #(l/log :error "Failed to get yougov-daily-total search results" (l/ex-log-msg %))))