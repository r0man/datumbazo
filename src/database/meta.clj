(ns database.meta
  (:require [clojure.java.jdbc :as jdbc]
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
  [atom ks v] (swap! atom assoc-in ks v) v)

(defn- lookup
  "Lookup `ks` in `atom`."
  [atom ks] (if ks (get-in @atom ks)))

;; SCHEMAS

(defrecord Schema [name]
  Nameable
  (as-keyword [schema]
    (jdbc/as-keyword (:name schema)))
  (as-identifier [schema]
    (jdbc/as-identifier (:name schema))))

(defn schema-key
  "Returns the lookup key for `schema` in *schema*"
  [schema] [(as-keyword schema)])

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
  (register-schema (Schema. (keyword (hyphenize name)))))

(defn load-schemas
  "Read the database schema from the current database connection."
  [] (->> (.getSchemas (.getMetaData (jdbc/connection)))
          (resultset-seq)
          (map #(make-schema (:table_schem %1)))))

;; TABLES

(defrecord Table [schema name columns]
  Nameable
  (as-identifier [table]
    (str (if (:schema table)
           (str (jdbc/as-identifier (:schema table)) "."))
         (jdbc/as-identifier (:name table)))))

(defn parse-table
  "Parse `s` as a table name with an optional schema at the beginning."
  [s] (let [[_ _ schema table] (re-matches #"(([^.]+)\.)?([^.]+)" (name s))]
        (->Table (as-keyword (or schema :public))
                 (as-keyword table) nil)))

(defn table-key
  "Returns the lookup key for `table` in *tables*."
  [{:keys [schema name] :as table}]
  (cond
   (and schema name)
   [(as-keyword (or schema :public))
    (as-keyword name)]
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
    (register-table
     (assoc (merge table options)
       :columns (apply vector (map :name columns))))))

;; COLUMNS

(defrecord Column [schema table name]
  Nameable
  (as-keyword [schema]
    (jdbc/as-keyword (:name schema)))
  (as-identifier [schema]
    (jdbc/as-identifier (:name schema))))

(defn column?
  "Returns true if arg is a column, otherwise false."
  [arg] (instance? Column arg))

(defn parse-column
  "Parse `s` as a database column."
  [s]
  (if-let [matches (re-matches #":?((.+)\.)?(.+)/(.+)" (str s))]
    (Column.
     (as-keyword (or (nth matches 2) :public))
     (as-keyword (nth matches 3))
     (as-keyword (nth matches 4)))))

(defn column-key
  "Returns the lookup key for `column` in *columns*."
  [{:keys [schema table name] :as column}]
  (cond
   (and table name)
   [(as-keyword (or schema :public))
    (as-keyword table)
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
  (register-column
   (assoc (merge (parse-column name) attributes)
     :type (keyword (if (sequential? type) (first type) type))
     :native? (if (sequential? type) false true)
     :not-null? (or (:not-null? attributes) (:primary-key? attributes)))))

(defn make-columns [table column-specs]
  (map #(apply make-column (str (:schema table) "." (:name table) "/" (name (first %1)))
               (rest %1)) column-specs))

;; INIT DEFAULT PUBLIC SCHEMA
(register-schema (make-schema :public))


(comment
  (database.connection/with-database :bs-database
    (load-schemas)))


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
