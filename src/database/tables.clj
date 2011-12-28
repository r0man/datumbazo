(ns database.tables
  (:refer-clojure :exclude (replace))
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (blank?)]
        [inflections.core :only (dasherize)]))

(defrecord Table [name type type-length default is-pk references not-null unique])

(defn table-name
  "Returns the name of the table as string."
  [table] (:name table))

(defn table-keyword
  "Returns the name of the table as keyword."
  [table] (keyword (dasherize (table-name table))))

(defn table-symbol
  "Returns the name of the table as symbol."
  [table] (symbol (dasherize (table-name table))))

(defn make-table
  "Make a new database table."
  [& {:as attributes}]
  (map->Table attributes))
