(ns datumbazo.util
  (:refer-clojure :exclude [replace])
  (:import java.io.File)
  (:require [clojure.java.io :refer [file reader]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? split replace trim]]
            [inflections.util :refer [parse-integer]]))

(defn absolute-path
  "Returns the absolute path of `path."
  [path] (.getAbsolutePath (file path)))

(defn clojure-file?
  "Returns true if `path` is a fixture file, otherwise false."
  [path]
  (and (.isFile (file path))
       (re-matches #"(?i).*\.cljs?$" (str path))))

(defn clojure-file-seq
  "Returns a tree seq of Clojure files in `directory`."
  [directory] (filter clojure-file? (file-seq (file directory))))

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

(defn parse-url
  "Parse `s` as a database url and return a JDBC/Ring compatible map."
  [s]
  (if-let [matches (re-matches #"(jdbc:)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))" (str s))]
    (let [db (nth matches 12)
          scheme (nth matches 2)
          server-name (nth matches 7)
          server-port (parse-integer (nth matches 9))]
      {:db db
       :host (nth matches 7)
       :password (nth matches 5)
       :port server-port
       :scheme scheme
       :server-name server-name
       :server-port server-port
       :subname (str "//" server-name (if server-port (str ":" server-port)) "/" db)
       :subprotocol scheme
       :uri (nth matches 11)
       :user (nth matches 4)
       :params (parse-params (nth matches 14))
       :query-string (nth matches 14)})))

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

(defn parse-subprotocol
  "Parse the JDBC subprotocol from `db-url`."
  [db-url]
  (if-let [matches (re-matches #"(([^:]+):)?([^:/]+):.+" (str db-url))]
    (nth matches 3)
    (illegal-argument-exception "Can't parse JDBC subprotocol: %s" db-url)))

(defn parse-db-url
  "Parse the database url `s` and return a Ring compatible map."
  [s]
  (if-let [matches (re-matches #"(([^:]+):)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))" (str s))]
    (let [db (nth matches 13)
          server-name (nth matches 8)
          server-port (parse-integer (nth matches 10))
          query-string (nth matches 15)]
      {:db db
       :params (parse-params query-string)
       :password (nth matches 6)
       :pool (keyword (or (nth matches 2) :jdbc))
       :query-string query-string
       :server-name server-name
       :server-port server-port
       :spec {:subname (str "//" server-name (if server-port (str ":" server-port)) "/" db (if-not (blank? query-string) (str "?" query-string)))
              :subprotocol (nth matches 3)
              :username (nth matches 5)
              :password (nth matches 6)}
       :uri (nth matches 12)
       :username (nth matches 5)})
    (illegal-argument-exception "Can't parse database connection url %s:" s)))

(defn slurp-sql
  [file]
  (with-open [reader (reader file)]
    (->> (line-seq reader)
         (map #(replace %1 #";$" ""))
         (doall))))

(defn exec-sql-file [file]
  (doseq [statement (slurp-sql file)]
    (jdbc/do-commands statement)))
