(ns database.tables
  (:require [database.columns :refer [column? column-name make-column]]
            [database.connection :refer [*naming-strategy*]]
            [clojure.string :refer [blank? split]]))

(defprotocol ITable
  (table-name [table]
    "Returns the table name.")
  (qualified-table-name [table]
    "Returns the qualified table name."))

(defrecord Table [schema name columns])

(defn column-prefix [table column]
  (keyword (str (table-name table) "-" (column-name column))))

(defn parse-table [table]
  (let [[_ _ schema table] (re-matches #"(([^.]+)\.)?([^.]+)" (name table))]
    (->Table schema table nil)))

(defn make-table
  "Make a new database table."
  [name & [columns & {:as options}]]
  (let [columns (map #(if (column? %1) %1 (apply make-column %1)) columns)]
    (merge (Table. (:schema options) name (zipmap (map :name columns) columns)) options)))

(defn make-table
  "Make a new database table."
  [name & [columns & {:as options}]]
  (let [table (parse-table name)
        columns (map #(if (column? %1) %1 (apply make-column %1)) columns)]
    (assoc (merge table options)
      :columns (zipmap (map :name columns) columns))))

(defn table?
  "Returns true if arg is a table, otherwise false."
  [arg] (instance? Table arg))

(defn table-identifier
  "Returns the table identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [table]
  (str (if (:schema table)
         (str ((:fields *naming-strategy*) (:schema table)) "."))
       ((:fields *naming-strategy*) (table-name table))))

(defn default-columns
  "Returns the default columns of `table`."
  [table] (vals (apply dissoc (:columns table) (:exclude (:fields table)))))

(defn select-columns
  "Select all columns of `table` which are in `selection`."
  [table selection]
  (let [selection (set (map column-name selection))]
    (filter #(contains? selection (column-name %1))
            (vals (apply dissoc (:columns table) (:exclude (:fields table)))))))

(defn primary-key-columns
  "Returns all primary key columns of `table`."
  [table] (filter :primary-key? (vals (:columns table))))

(defn unique-columns
  "Returns all unique columns of `table`."
  [table] (filter :unique? (vals (:columns table))))

(defn key-columns
  "Returns the key columns of `table` for `record`."
  [table record]
  (let [key-set (set (keys record))]
    (filter #(contains? key-set (:name %1))
            (concat (primary-key-columns table)
                    (unique-columns table)))))

(extend-type Table
  ITable
  (table-name [table]
    (table-name (:name table)))
  (qualified-table-name [table]
    (str (if (:schema table)
           (str (name (:schema table)) "."))
         (name (:name table)))))

(extend-type String
  ITable
  (table-name [string]
    string)
  (qualified-table-name [string]
    string))

(extend-type clojure.lang.IPersistentMap
  ITable
  (table-name [table]
    (if-not (blank? (:name table))
      (table-name (:name table))
      (throw (IllegalArgumentException. (format "Not a table: %s" (prn-str table))))))
  (qualified-table-name [table]
    (if-not (blank? (:name table))
      (qualified-table-name (:name table))
      (throw (IllegalArgumentException. (format "Not a table: %s" (prn-str table)))))))

(extend-type clojure.lang.Keyword
  ITable
  (table-name [keyword]
    (name keyword))
  (qualified-table-name [keyword]
    (name keyword)))

(extend-type clojure.lang.Symbol
  ITable
  (table-name [symbol]
    (str symbol))
  (qualified-table-name [symbol]
    (str symbol)))
