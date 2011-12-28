(ns database.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use [database.columns :only (make-column)]
        [database.registry :only (register-table)]
        [inflections.core :only (dasherize)]))

(defrecord Table [name columns])

(defn make-table
  "Make a new database table."
  [& {:as attributes}]
  (map->Table
   (assoc attributes
     :name (keyword (:name attributes)))))

(defn table-name
  "Returns the name of the table as string."
  [table] (jdbc/as-identifier (:name table)))

(defn table-keyword
  "Returns the name of the table as keyword."
  [table] (keyword (dasherize (:name table))))

(defn table-symbol
  "Returns the name of the table as symbol."
  [table] (symbol (name (dasherize (:name table)))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (let [columns# (map #(apply make-column %1) columns)
        table# (make-table :name name :columns columns#)]
    (register-table table#)))
