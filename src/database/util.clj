(ns database.util)

(defn assoc-url
  "Assoc the result of applying `record` to `url-fn` under the :url
  key onto `record`."
  [record url-fn]
  (if url-fn
    (if-let [url (url-fn record)]
      (assoc record :url url) record)
    record))

(defn parse-integer
  "Parse `string` as an integer."
  [string & {:keys [junk-allowed radix]}]
  (if (integer? string)
    (int string)
    (try (Integer/parseInt (first (re-find #"([+-]?\d+)" string)) (or radix 10))
         (catch Exception e
           (when-not junk-allowed
             (throw e))))))

(defn split-args [args]
  (let [[args options]
        [(take-while (complement keyword?) args)
         (drop-while (complement keyword?) args)]]
    (if (odd? (count options))
      [(concat args [(first options)]) (rest options)]
      [args options])))
