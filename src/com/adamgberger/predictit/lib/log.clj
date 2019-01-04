(ns com.adamgberger.predictit.lib.log
  (:require [clojure.core.async :as async])
  (:gen-class))

(def log-chan (async/chan 100))

(async/go-loop []
    (println (async/<! log-chan))
    (recur))

(defn log 
    ([level msg]
        (log level msg {}))
    ([level msg extra]
        (let [ts (-> (java.time.Instant/now) str)
            msg (merge {:level level :msg msg} extra {:ts ts})]
            (async/>!! log-chan msg))))

(defn ex-log-msg [ex]
    {:stack (clojure.string/join "  <-  " (.getStackTrace ex))
     :ex-data (ex-data ex)
     :ex-msg (.getMessage ex)
     :ex-cls (-> ex .getClass .getCanonicalName)})

(defn logging-agent [n agt]
    (set-error-handler!
        agt
        #(log :error "Agent error" (merge {:agent n} (ex-log-msg %2))))
    agt)

(defmacro with-log [level msg & body]
    `(try
        (let [start# (java.time.Instant/now)
              data# (do ~@body)]
            (log ~level ~msg {:data data#
                              :elapsed-ms (.until start# (java.time.Instant/now) java.time.temporal.ChronoUnit/MILLIS)})
            data#)
        (catch Exception ex#
            (log
                :error
                (str "EXCEPTION while running: " ~msg)
                (ex-log-msg ex#))
            (throw ex#))))