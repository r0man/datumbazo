(ns database.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use [database.columns :only (make-column)]
        [database.registry :only (register-table)]
        [inflections.core :only (dasherize)]))

(defrecord Table [name columns])

(defn make-table
  "Make a new database table."
  [& {:as attributes}]
  (assoc (map->Table (or attributes {}))
    :name (keyword (:name attributes))))

(defn table-identifier
  "Returns the table identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [table] (jdbc/as-identifier (:name table)))

(defn table-keyword
  "Returns the name of the table as a keyword with all underscores in
  the name replaced by dashes."
  [table] (keyword (dasherize (:name table))))

(defn table-symbol
  "Returns the name of the table as a symbol with all underscores in
  the name replaced by dashes."
  [table] (symbol (name (dasherize (:name table)))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (->> (map #(apply make-column %1) columns)
       (make-table :name name :columns)
       (register-table)))
