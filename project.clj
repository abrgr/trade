(defproject com.adamgberger/predictit "0.1.0-SNAPSHOT"
  :description "Predictit Trading"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-http "3.9.1"]
                 [org.clojure/data.json "0.2.6"]]
  :main ^:skip-aot com.adamgberger.predictit.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
