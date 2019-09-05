(defproject com.adamgberger/predictit "0.1.0-SNAPSHOT"
  :description "Predictit Trading"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.memoize "0.7.1"]
                 [org.clojure/core.cache "0.7.1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/core.async "0.4.490"]
                 [org.apache.commons/commons-math3 "3.6.1"]
                 [org.apache.tika/tika-core "1.20"]
                 [org.apache.tika/tika-parsers "1.20"]
                 [clj-http "3.9.1"]
                 [hickory "0.7.1"]
                 [org.clojure/data.csv "0.1.4"]
                 [iapetos "0.1.6"]
                 [io.prometheus/simpleclient_hotspot "0.0.14"]
                 [de.xypron.jcobyla/jcobyla "1.3"]]
  :plugins [[jonase/eastwood "0.3.4"]
            [lein-cljfmt "0.6.4"]]
  :main ^:skip-aot com.adamgberger.predictit.main
  :target-path "target/%s"
  :resource-paths ["/scratch/Ipopt-3.12.13/build/Ipopt/contrib/JavaInterface"
                   "/scratch/jom/jom-0.4.0.jar"
                   "/scratch/jom/parallelcolt-0.9.4.jar"]
  :profiles {:uberjar {:aot :all}})
