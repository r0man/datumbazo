(ns database.tables
  (:use [clojure.string :only (join)]
        [database.columns :only (column? make-column)]
        [inflections.core :only (dasherize)]
        database.columns
        database.connection))

(defprotocol ITable
  (table-name [table]
    "Returns the name of the table as a string."))

(defrecord Table [name columns])

(defn make-table
  "Make a new database table."
  [name & [columns & {:as options}]]
  (let [columns (map #(if (column? %1) %1 (apply make-column %1)) columns)]
    (merge (Table. name (zipmap (map :name columns) columns)) options)))

(defn table?
  "Returns true if arg is a table, otherwise false."
  [arg] (instance? Table arg))

(defn table-identifier
  "Returns the table identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [table] ((:fields (naming-strategy)) (table-name table)))

(extend-type Table
  ITable
  (table-name [table]
    (table-name (:name table))))

(extend-type String
  ITable
  (table-name [string]
    string))

(extend-type clojure.lang.IPersistentMap
  ITable
  (table-name [table]
    (if-let [name (:name table)]
      (table-name name)
      (throw (IllegalArgumentException. (format "Not a table: %s" (prn-str table)))))))

(extend-type clojure.lang.Keyword
  ITable
  (table-name [keyword]
    (name keyword)))

(extend-type clojure.lang.Symbol
  ITable
  (table-name [symbol]
    (str symbol)))