(ns database.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use [database.columns :only (make-column)]
        [inflections.core :only (dasherize)]))

(defonce ^:dynamic *tables* (atom {}))

(defrecord Table [name columns])

(defn make-table
  "Make a new database table."
  [& {:as attributes}]
  (assoc (map->Table (or attributes {}))
    :name (keyword (:name attributes))))

(defn table?
  "Returns true if arg is a table, otherwise false."
  [arg] (instance? Table arg))

(defn table-identifier
  "Returns the table identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [table] (jdbc/as-identifier (if (table? table) (:name table) table)))

(defn table-symbol
  "Returns the name of the table as a symbol with all underscores in
  the name replaced by dashes."
  [table] (symbol (name (dasherize (:name table)))))

(defn table-keyword
  "Returns the name of the table as a keyword with all underscores in
  the name replaced by dashes."
  [table] (keyword (table-symbol table)))

(defn find-table
  "Find the database table in *tables* by it's name."
  [name] (get @*tables* (table-keyword {:name name})))

(defn register-table
  "Register the database table in *tables*."
  [table] (swap! *tables* assoc (table-keyword table) table))
