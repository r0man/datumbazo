(ns database.meta
  (:require [clj-time.core :refer [now]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [upper-case]]
            [clojure.tools.logging :refer [info]]
            [database.protocol :refer [as-identifier as-keyword]]
            [inflections.core :refer [hyphenize]])
  (:import database.protocol.Nameable))

(defonce ^:dynamic *schemas*
  (atom {}))

(defonce ^:dynamic *columns*
  (atom {}))

(defonce ^:dynamic *tables*
  (atom {}))

(defn- register
  "Register `v` in `atom` under `ks` and and return `v`."
  [atom ks v]
  (let [v (assoc v :registered-at (now))]
    (swap! atom assoc-in ks v)
    v))

(defn- lookup
  "Lookup `ks` in `atom`."
  [atom ks] (if ks (get-in @atom ks)))

;; SCHEMAS

(defrecord Schema [table-schem]
  Nameable
  (as-keyword [schema]
    (keyword (hyphenize table-schem)))
  (as-identifier [schema]
    (jdbc/as-identifier table-schem)))

(defn schema?
  "Returns true if `arg` is a Schema, otherwise false."
  [arg] (instance? Schema arg))

(defn schema-key
  "Returns the lookup key for `schema` in *schema*"
  [schema] (if schema [(as-keyword schema)]))

(defn register-schema
  "Register the database schema in *schemas*."
  [schema] (register *schemas* (schema-key schema) schema))

(defn lookup-schema
  "Lookup `schema` in *schemas*."
  [schema] (lookup *schemas* (schema-key schema)))

(defn make-schema
  "Make a new database schema named `name`."
  [name]
  (assert (keyword name) (str "Invalid schema name: " (prn-str name)))
  (Schema. (as-identifier name)))

(defn read-schemas
  "Read the schema meta data from the current database connection."
  []
  (->> (.getSchemas (.getMetaData (jdbc/connection)))
       (jdbc/resultset-seq)
       (map #(make-schema (:table_schem %1)))))

(defn load-schemas
  "Load the database schema from the current database connection."
  [] (doall (map register-schema (read-schemas))))

;; TABLES

(defrecord Table [table-schem table-name]
  Nameable
  (as-keyword [table]
    (keyword (hyphenize (:table-schem table))))
  (as-identifier [table]
    (str (if (:table-schem table)
           (str (jdbc/as-identifier (:table-schem table)) "."))
         (jdbc/as-identifier (:name table)))))

(defn table?
  "Returns true if `arg` is a Table, otherwise false."
  [arg] (instance? Table arg))

(defn parse-table
  "Parse `s` as a table name with an optional schema at the beginning."
  [s] (let [[_ _ schema table] (re-matches #"(([^.]+)\.)?([^.]+)" (name s))]
        (->Table (as-identifier (or schema "public"))
                 (as-identifier table))))

(defn table-key
  "Returns the lookup key for `table` in *tables*."
  [{:keys [table-schem table-name] :as table}]
  (cond
   (and table-schem table-name)
   [(as-keyword (or table-schem :public))
    (as-keyword table-name)]
   (keyword? table)
   (table-key (parse-table table))))

(defn register-table
  "Register the database table in *tables*."
  [table] (register *tables* (table-key table) table))

(defn lookup-table
  "Lookup `table` in *tables*."
  [table] (lookup *tables* (table-key table)))

(declare make-columns)

(defn make-table
  "Make a new database table."
  [name & [columns & {:as options}]]
  (let [table (parse-table name)
        columns (make-columns table columns)]
    (assoc (merge table options)
      :columns (apply vector (map :name columns)))))

(defn read-tables
  "Read the table meta data form the current database connection."
  [& {:keys [catalog schema table]}]
  (->> (.getTables (.getMetaData (jdbc/connection))
                   (if catalog (name catalog))
                   (if schema (name schema))
                   (if table (name table))
                   (into-array String ["TABLE"]))
       (jdbc/resultset-seq)
       (map #(map->Table (hyphenize %1)))))

(defn load-tables
  "Load the tables from the current database connection."
  [& options]
  (->> (for [table (apply read-tables options)
             :let [table (make-table (str (:table_schem table) "." (:table_name table)) [])]]
         (do (info (format "Loading database meta data for table %s." (as-identifier table)))
             table))
       (doall)))

;; COLUMNS

(defrecord Column [table-schem table-name name]
  Nameable
  (as-keyword [schema]
    (jdbc/as-keyword (:name schema)))
  (as-identifier [schema]
    (jdbc/as-identifier (:name schema))))

(defn column?
  "Returns true if `arg` is a Column, otherwise false."
  [arg] (instance? Column arg))

(defn parse-column
  "Parse `s` as a database column."
  [s]
  (if-let [matches (re-matches #":?((.+)\.)?(.+)/(.+)" (str s))]
    (Column.
     (as-identifier (or (nth matches 2) :public))
     (as-identifier(nth matches 3))
     (as-identifier (nth matches 4)))))

(defn column-key
  "Returns the lookup key for `column` in *columns*."
  [{:keys [table-schem table-name name] :as column}]
  (cond
   (and table-name name)
   [(as-keyword (or table-schem :public))
    (as-keyword table-name)
    (as-keyword name)]
   (keyword? column)
   (column-key (parse-column column))))

(defn register-column
  "Register the database column in *columns*."
  [column] (register *columns* (column-key column) column))

(defn lookup-column
  "Lookup `column` in *columns*."
  [column] (lookup *columns* (column-key column)))

(defn make-column
  "Make a new database column."
  [name type & {:as attributes}]
  (assoc (merge (parse-column name) attributes)
    :type (keyword (if (sequential? type) (first type) type))
    :native? (if (sequential? type) false true)
    :not-null? (or (:not-null? attributes) (:primary-key? attributes))))

(defn make-columns [table column-specs]
  (map #(apply make-column (str (:table-schem table) "." (:name table) "/" (name (first %1)))
               (rest %1)) column-specs))

(defn read-columns
  "Read the column meta data from the current database connection."
  [& {:keys [catalog schema table column]
      :or {schema :public types [:table]}}]
  (->> (.getColumns (.getMetaData (jdbc/connection))
                    (if catalog (name catalog))
                    (if schema (name schema))
                    (if table (name table))
                    (if column (name column)))
       (jdbc/resultset-seq)
       (map #(map->Column (hyphenize %1)))))

;; INIT DEFAULT PUBLIC SCHEMA
(register-schema (make-schema :public))

;; (lookup-table :spot-weather)

;; (database.connection/with-database :bs-database
;;   (clojure.pprint/pprint (first (read-columns))))

;; (database.connection/with-database :bs-database
;;   (prn (read-tables)))

;; (comment
;;   (database.connection/with-database :bs-database
;;     (load-tables)))


;; (defn tables
;;   "Retrieves the schemas available to the database connection."
;;   [connection & {:keys [catalog schema-pattern table-pattern types]
;;                  :or {schema-pattern :public types [:table]}}]
;;   (->> (.getTables (.getMetaData connection)
;;                    (if catalog (name catalog))
;;                    (if schema-pattern (name schema-pattern))
;;                    (if table-pattern (name table-pattern))
;;                    (into-array String (map (comp upper-case name) (remove nil? types))))
;;        (resultset-seq)))

;; (defn info
;;   "Returns the product name, mayor and minor version of the database."
;;   [connection]
;;   (let [meta (.getMetaData connection)]
;;     {:product-name (.getDatabaseProductName meta)
;;      :major-version (.getDatabaseMajorVersion meta)
;;      :minor-version (.getDatabaseMinorVersion meta)}))

;; ;; (with-database :bs-database
;; ;;   (schemas (jdbc/find-connection)))

;; ;; (jdbc/connection)

(defmacro with-table
  "Evaluate `body` with a resolved `table`."
  [[sym table] & body]
  (let [table# table]
    `(if-let [~sym (lookup-table ~table#)]
       (do ~@body) (throw (Exception. (str "Table not found: " ~table#))))))

(defmacro with-ensure-column
  "Evaluate body with a resolved `table` and `column`."
  [[table [sym column]] & body]
  (let [column# column table# (gensym)]
    `(with-ensure-table [~table# ~table]
       (let [~sym (if (column? ~column#) ~column# (get (:columns ~table#) ~column#))
             ~sym (assoc ~sym :table ~table#)]
         (assert (column? ~sym))
         ~@body))))