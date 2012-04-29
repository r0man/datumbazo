(ns database.util
  (:refer-clojure :exclude (replace))
  (:use [clojure.string :only (replace)]))

(defn dissoc-if [map pred & keys]
  (reduce
   #(if (pred (get %1 %2)) (dissoc %1 %2) %1)
   map keys))

(defn immigrate
  "Create a public var in this namespace for each public var in the
namespaces named by ns-names. The created vars have the same name, root
binding, and metadata as the original except that their :ns metadata
value is this namespace."
  [& ns-names]
  (doseq [ns ns-names]
    (require ns)
    (doseq [[sym var] (ns-publics ns)]
      (let [v (if (.hasRoot var)
                (var-get var))
            var-obj (if v (intern *ns* sym v))]
        (when var-obj
          (alter-meta! var-obj
                       (fn [old] (merge (meta var) old)))
          var-obj)))))

(defn parse-float
  "Parse `string` as a float."
  [string & {:keys [junk-allowed]}]
  (if (float? string)
    (float string)
    (try (Float/parseFloat string)
         (catch Exception e
           (when-not junk-allowed
             (throw e))))))

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
      [(concat args [(first options)])
       (apply hash-map (rest options))]
      [args (apply hash-map options)])))

(defn shift-columns
  "Shift all columns in record starting with prefix into a sub map
  under `path` in `record`."
  [record prefix & path]
  (if prefix
    (let [path (concat path [prefix])
          keys (filter #(.startsWith (str %) (str prefix)) (keys record))]
      (reduce
       #(-> (if-let [value (get %1 %2)]
              (assoc-in %1 (concat path [(keyword (apply str (rest (replace (str %2) (str prefix) ""))))]) value)
              %1)
            (dissoc %2)) (into {} record) keys))
    record))

(defn prefix-keywords
  "Prefix all `keywords` by `prefix` and join them with a blank or the
  given separator."
  [prefix keywords & [separator]]
  (let [separator (or separator "")]
    (map #(keyword (str (name prefix) separator (name %1))) keywords)))
