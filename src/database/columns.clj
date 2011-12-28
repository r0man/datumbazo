(ns database.columns
  (:refer-clojure :exclude (replace))
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (blank?)]
        [inflections.core :only (dasherize)]))

(defrecord Column [name type type-length default is-pk references not-null unique])

(defn column-name
  "Returns the name of the column as string."
  [column] (:name column))

(defn column-keyword
  "Returns the name of the column as keyword."
  [column] (keyword (dasherize (column-name column))))

(defn column-symbol
  "Returns the name of the column as symbol."
  [column] (symbol (dasherize (column-name column))))

(defn make-column
  "Make a new database column."
  [& {:as attributes}]
  (map->Column attributes))
