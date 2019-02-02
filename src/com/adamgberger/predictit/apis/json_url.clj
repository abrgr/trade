(ns com.adamgberger.predictit.apis.json-url
  (:require [clj-http.client :as h]
            [clj-http.conn-mgr :as conn-mgr]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [com.adamgberger.predictit.lib.log :as l]
            [com.adamgberger.predictit.lib.utils :as utils])
  (:gen-class))

(def cm (conn-mgr/make-reusable-conn-manager {:timeout 10 :threads 20}))

(defn async-get-json
    ([browser-url url value-fns cb cb-err]
        (async-get-json browser-url url value-fns identity cb cb-err))
    ([browser-url url value-fns raw-xform cb cb-err]
        (let [opts {:async? true
                    :connection-manager cm
                    :headers {"User-Agent" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
                                "Accept-Language" "en-US,en;q=0.9"
                                "Accept" "application/json, text/plain, */*"
                                "Cache-Control" "no-cache"
                                "Pragma" "no-cache"
                                "Referer" browser-url}}]
            (h/get
                url
                opts
                #(-> % 
                     :body
                     raw-xform
                     (utils/read-json value-fns)
                     cb)
                cb-err))))

(defn get-json 
    ([browser-url url value-fns]
        (get-json browser-url url value-fns identity))
    ([browser-url url value-fns raw-xform]
        (let [opts {:connection-manager cm
                    :headers {"User-Agent" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
                                "Accept-Language" "en-US,en;q=0.9"
                                "Accept" "application/json, text/plain, */*"
                                "Cache-Control" "no-cache"
                                "Pragma" "no-cache"
                                "Referer" browser-url}}]
            (-> (h/get url opts)
                :body
                raw-xform
                (utils/read-json value-fns)))))