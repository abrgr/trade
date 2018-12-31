(ns com.adamgberger.predictit.lib.utils
  (:gen-class))

(declare merge-deep)

(defn deep-merger [v1 v2]
  (if (and (map? v2) (map? v2))
    (merge-deep v1 v2)
    v2))

(defn merge-deep [& maps]
  (apply merge-with deep-merger maps))

(defn split-first [haystack needle]
  (let [idx (.indexOf haystack needle)]
    (if (neg? idx)
      [haystack ""]
      [(.substring haystack 0 idx) (.substring haystack (inc idx))])))