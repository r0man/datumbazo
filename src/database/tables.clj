(ns database.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (join)]
        [database.columns :only (column? make-column)]
        [inflections.core :only (dasherize)]
        database.columns
        database.connection))

(defonce ^:dynamic *tables* (atom {}))

(defrecord Table [name columns])

(defn make-table
  "Make a new database table."
  [name & [columns]]
  (let [columns (map #(if (column? %1) %1 (apply make-column %1)) columns)]
    (Table. name (zipmap (map :name columns) columns))))

(defn table?
  "Returns true if arg is a table, otherwise false."
  [arg] (instance? Table arg))

(defn table-name
  "Returns the name of the table."
  [table] (if (table? table) (:name table) table))

(defn table-identifier
  "Returns the table identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [table] ((:fields (naming-strategy)) (table-name table)))

(defn table-symbol
  "Returns the name of the table as a symbol with all underscores in
  the name replaced by dashes."
  [table] (symbol (name (dasherize (table-name table)))))

(defn table-keyword
  "Returns the name of the table as a keyword with all underscores in
  the name replaced by dashes."
  [table] (keyword (table-symbol table)))

(defn find-table
  "Find the database table in *tables* by it's name."
  [name] (get @*tables* (table-keyword name)))

(defn register-table
  "Register the database table in *tables*."
  [table]
  (swap! *tables* assoc (table-keyword table) table)
  table)

(defmacro with-ensure-table
  "Evaluate body with a resolved table."
  [table & body]
  (let [table# table]
    `(if-let [~table# (find-table ~table#)]
       (do ~@body) (throw (Exception. "Table not found.")))))