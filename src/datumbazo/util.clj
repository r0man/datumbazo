(ns datumbazo.util
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.io :refer [file reader]]
            [clojure.string :as str :refer [blank? replace split]]
            [datumbazo.callbacks :as callback]
            [datumbazo.connection :as connection]
            [no.en.core :as noencore]
            [datumbazo.driver.core :as driver]
            [schema.core :as s]
            [inflections.core :as infl]
            [sqlingvo.core :as sql])
  (:import java.io.File
           java.sql.SQLException
           [java.util List Map]
           sqlingvo.db.Database))

(def ^:private jdbc-url-regex
  "The regular expression to match JDBC urls."
  #"(([^:]+):)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))")

(defn parse-url
  "Parse the database `url` and return a Ring compatible map."
  [url]
  (if-let [matches (re-matches jdbc-url-regex (str url))]
    (let [database (nth matches 13)
          server-name (nth matches 8)
          server-port (noencore/parse-integer (nth matches 10))
          query-string (nth matches 15)]
      {:name database
       :password (nth matches 6)
       :pool (keyword (nth matches 2))
       :query-params (noencore/parse-query-params query-string)
       :scheme (keyword (nth matches 3))
       :server-name server-name
       :server-port server-port
       :username (nth matches 5)})
    (throw (ex-info "Can't parse JDBC url %s." {:url url}))))

(defn format-url
  "Format the `db` spec as a URL."
  [db]
  (let [spec (assoc db :uri (str "/" (:name db)))]
    (noencore/format-url spec)))

(defmulti table-by-class
  "Return the table definition for `class`."
  (fn [class] class))

(defn absolute-path
  "Returns the absolute path of `path."
  [path] (.getAbsolutePath (file path)))

(defn class-symbol
  "Convert `table` into the type symbol."
  [table]
  (-> (infl/singular (-> table :name name))
      (infl/camel-case)
      (symbol)))

(s/defn columns-by-class :- #{Map}
  "Return the columns of a table by it's `class`."
  [class :- Class]
  (let [table (table-by-class class)]
    (set (map (:column table) (:columns table)))))

(defn current-user
  "Returns the USER environment variable."
  [] (System/getenv "USER"))

(defn compact-map
  "Returns a map with all key/value pairs removed where the value of
  the pair is nil."
  [m]
  (reduce
   (fn [memo key]
     (let [value (get m key)]
       (cond
         (nil? value) memo
         (map? value)
         (assoc memo key (compact-map value))
         :else (assoc memo key value))))
   {} (keys m)))

(defn edn-file?
  "Returns true if `path` is a EDN file, otherwise false."
  [path]
  (and (.isFile (file path))
       (re-matches #"(?i).*\.edn$" (str path))))

(defn edn-file-seq
  "Returns a tree seq of Clojure files in `directory`."
  [directory] (filter edn-file? (file-seq (file directory))))

(defn illegal-argument-exception
  "Throw an IllegalArgumentException with a formatted message."
  [format-message & format-args]
  (throw (IllegalArgumentException. (apply format format-message format-args))))

(defn invoke-constructor [clazz & args]
  (clojure.lang.Reflector/invokeConstructor
   (Class/forName (str clazz)) (into-array args)))

(defn format-server [url]
  (str (:server-name url)
       (if (:server-port url)
         (str ":" (:server-port url)))))

(defmulti make-instance
  "Make a new instance of `class` using `attrs`."
  (fn [class attrs & [db]] class))

(defmethod make-instance :default
  [class attrs & [db]]
  (throw (ex-info (str "Can't make instance of " class ".")
                  {:attrs attrs
                   :class class
                   :db db})))

(s/defn make-instances :- [Map]
  "Convert all `records` into instances of `class`."
  [db :- Database class :- Class records :- [Map]]
  (map #(make-instance class % db (meta %)) records))

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

(defn path-split
  "Split `s` at the file separator."
  [s] (split (str s) (re-pattern File/separator)))

(defn path-replace
  "Absolute path substitude `match` in `s` with `replacement`."
  [s match & [replacement]]
  (replace (absolute-path s)
           (str (absolute-path match) "/")
           (or replacement "")))

(defmacro defn-memo
  "Just like defn, but memoizes the function using clojure.core/memoize"
  [fn-name & defn-stuff]
  `(do
     (defn ~fn-name ~@defn-stuff)
     (alter-var-root (var ~fn-name) memoize)
     (var ~fn-name)))

(defn parse-params
  "Parse `s` as a query string and return a hash map."
  [s] (->> (split (or s "") #"&")
           (remove blank?)
           (map #(split %1 #"="))
           (mapcat #(vector (keyword (first %1)) (second %1)))
           (apply hash-map)))

(defn qualified-name
  "Returns the qualified name of `k`."
  [k] (replace (str k) #"^:" ""))

(defn parse-schema
  "Parse the schema `s` and return a map with the :name key."
  [s] {:name (keyword (qualified-name s))})

(defn parse-column
  "Parse the column `s` and return a map with :schema, :table and :name keys."
  [s]
  (let [parts (map keyword (split (qualified-name s) #"\.|/" 3))]
    (condp = (count parts)
      1 {:name (first parts)}
      2 (zipmap [:table :name] parts)
      3 (zipmap [:schema :table :name] parts)
      :else (throw (illegal-argument-exception "Can't parse column: %s" s)))))

(defn slurp-sql
  [file]
  (with-open [reader (reader file)]
    (->> (line-seq reader)
         (map #(replace %1 #";$" ""))
         (doall))))

(defn- parse-command
  "Parse the SQL statement `s` and return the type of SQL command as a
  keyword."
  [s]
  (some-> (re-matches #"\s*([^ ]+).*" (str s))
          (nth 1 nil)
          (str/lower-case)
          (keyword)))

(s/defn resolve-class :- Class
  "Require the namespace of `class` and resolve the `class` in it."
  [class :- s/Str]
  (when-let [[_ ns class] (re-matches #"(.+)\.([^.]+)" class)]
    (require (symbol ns))
    (ns-resolve (symbol ns) (symbol class))))

(defn take-upto
  "Returns a lazy sequence of successive items from coll up to and including
  the first item for which `(pred item)` returns true."
  ([pred]
   (fn [rf]
     (fn
       ([] (rf))
       ([result] (rf result))
       ([result x]
        (let [result (rf result x)]
          (if (pred x)
            (ensure-reduced result)
            result))))))
  ([pred coll]
   (lazy-seq
    (when-let [s (seq coll)]
      (let [x (first s)]
        (cons x (if-not (pred x) (take-upto pred (rest s)))))))))

(defn sql-stmt-seq
  "Return a seq of SQL statements from `reader`."
  [reader]
  (letfn [(lazy-stmts [lines]
            (lazy-seq
             (let [parts (take-upto #(str/ends-with? (str %) ";") lines)
                   stmt (str/join " " parts)
                   lines (drop (count parts) lines) ]
               (if (seq lines)
                 (cons stmt (lazy-stmts lines))
                 (list stmt)))))]
    (lazy-stmts
     (->> (line-seq reader)
          (map str/trim)
          (remove str/blank?)))))

(defn exec-sql-file
  "Slurp `file` and execute each line as a statement."
  [db file]
  (with-open [reader (reader file)]
    (doseq [statement (sql-stmt-seq reader)
            :let [statement (replace statement #";$" "")]]
      (case (parse-command statement)
        :select (connection/execute-sql-query db [statement] nil)
        (connection/execute-sql-statement db [statement] nil)))
    file))

(defn throw-sql-ex-info
  "Throw `e` with `sql` and the next exception in `ex-data`."
  [e sql]
  (throw (ex-info
          (.getMessage e)
          {:next (if (instance? SQLException e)
                   (.getNextException e))
           :sql sql}
          (.getCause e))))

(defn fetch-batch
  "Return a lazy seq that fetches the result set of `stmt` in batches
  of `size`."
  [stmt & [{:keys [size]}]]
  (letfn [(next-batch [offset limit]
            (lazy-seq
             (let [result @(sql/select
                               (-> stmt sql/ast :db) [:*]
                             (sql/from (sql/as stmt (keyword (gensym 'batch-))))
                             (sql/offset offset)
                             (sql/limit limit))
                   result-size (count result)]
               (cond
                 (= result-size limit)
                 (concat result (next-batch (+ offset limit) limit))
                 (< result-size limit)
                 result
                 :else nil))))]
    (next-batch 0 (or size 3))))

(defn table-keyword
  "Return the schema qualified `table` keyword."
  [table]
  (keyword (str (if-let [schema (:schema table)]
                  (str (name schema) "."))
                (-> table :name name))))

(defn column-keyword
  "Return the column as keyword."
  [column & [include-table?]]
  (keyword (str (when include-table?
                  (str (name (:table column)) "."))
                (name (:name column)))))
