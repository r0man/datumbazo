(ns database.tables
  (:use [database.columns :only (column? column-name make-column)]
        [database.connection :only (naming-strategy)]))

(defprotocol ITable
  (table-name [table]
    "Returns the name of the table as a string."))

(defrecord Table [name columns])

(defn column-prefix [table column]
  (keyword (str (table-name table) "-" (column-name column))))

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
  (let [columns (select-columns table (keys record))]
    (concat (filter :primary-key? columns) (filter :unique? columns))))

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
