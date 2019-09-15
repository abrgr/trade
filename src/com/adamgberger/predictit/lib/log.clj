(ns com.adamgberger.predictit.lib.log
  (:require [clojure.string :as string]
            [clojure.core.async :as async])
  (:gen-class))

(def log-chan (async/chan 100))

(async/go-loop []
  (println (pr-str (async/<! log-chan)))
  (recur))

(def ^:dynamic *skipped-log-levels* #{:debug :trace})
(def ^:dynamic *replay-triggering-log-levels* #{:warn :error :fatal})
(def ^:dynamic *lookback-window* 50)

(def ^:private lookback-window (atom '()))

(defn log
  ([level msg]
   (log level msg {}))
  ([level msg extra]
   (let [ts (-> (java.time.Instant/now) str)
         msg (merge {:level level :msg msg} extra {:ts ts})]
     (swap! lookback-window #(take *lookback-window* (conj % msg)))
     (when-not (*skipped-log-levels* level)
       (async/>!! log-chan msg)
       (when (*replay-triggering-log-levels* level)
         (async/>!! log-chan {:level level :msg "Log replay" :ts ts :replay (into [] @lookback-window)}))))))

(defn ex-log-msg [^Exception ex]
  {:stack (string/join "  <-  " (.getStackTrace ex))
   :ex-data (ex-data ex)
   :ex-msg (.getMessage ex)
   :ex-cls (-> ex .getClass .getCanonicalName)})

(defn logging-agent [n agt]
  (set-error-handler!
   agt
   #(log :error "Agent error" (merge {:agent n} (ex-log-msg %2))))
  (set-error-mode! agt :continue)
  agt)

(defmacro with-log [level msg & body]
  `(try
     (let [start# (java.time.Instant/now)
           data# (do ~@body)]
       (log ~level ~msg {:elapsed-ms (.until start# (java.time.Instant/now) java.time.temporal.ChronoUnit/MILLIS)})
       data#)
     (catch Exception ex#
       (log
        :error
        (str "EXCEPTION while running: " ~msg)
        (ex-log-msg ex#))
       (throw ex#))))
