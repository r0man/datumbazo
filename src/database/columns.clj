(ns database.columns
  (:require [clojure.java.jdbc :as jdbc])
  (:use [inflections.core :only (dasherize)]))

(defrecord Column [name type type-length default not-null primary-key references unique])

(defn make-column
  "Make a new database column."
  [name type & {:as attributes}]
  (map->Column
   (assoc attributes
     :name (keyword name)
     :type (keyword type)
     :not-null (or (:not-null attributes) (:primary-key attributes)))))

(defn column-name
  "Returns the name of the column as string."
  [column] (jdbc/as-identifier (:name column)))

(defn column-keyword
  "Returns the name of the column as keyword."
  [column] (keyword (dasherize (:name column))))

(defn column-symbol
  "Returns the name of the column as symbol."
  [column] (symbol (name (dasherize (:name column)))))
