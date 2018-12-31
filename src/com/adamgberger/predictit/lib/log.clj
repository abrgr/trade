(ns com.adamgberger.predictit.lib.log
  (:gen-class))

(defn log [level msg extra]
    (let [ts (-> (java.time.Instant/now) str)
          msg (merge {:level level :msg msg} extra {:ts ts})]
        (println msg)))

(defn ex-log-msg [ex]
    {:stack (clojure.string/join "  <-  " (.getStackTrace ex))
     :ex-data (ex-data ex)
     :ex-msg (.getMessage ex)
     :ex-cls (-> ex .getClass .getCanonicalName)})

(defmacro with-log [level msg & body]
    `(try
        (let [data# (do ~@body)]
            (log ~level ~msg {:data data#})
            data#)
        (catch Exception ex#
            (log
                :error
                (str "EXCEPTION while running: " ~msg)
                (ex-log-msg ex#))
            (throw ex#))))