(ns database.columns
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (split)]
        [inflections.core :only (dasherize)]))

(defrecord Column [name type length default not-null primary-key references unique])

(defn make-column
  "Make a new database column."
  [name type & {:as attributes}]
  (assoc (map->Column (or attributes {}))
    :name (keyword name)
    :type (keyword type)
    :not-null (or (:not-null attributes) (:primary-key attributes))))

(defn column-name
  "Returns the name of the column as string."
  [column] (jdbc/as-identifier (:name column)))

(defn column-keyword
  "Returns the name of the column as keyword."
  [column] (keyword (dasherize (:name column))))

(defn column-symbol
  "Returns the name of the column as symbol."
  [column] (symbol (name (dasherize (:name column)))))

(defn- references-clause [column]
  (if-let [reference (:references column)]
    (->> (split (replace (str reference) #":" "") #"/")
         (map column-name)
         (apply format "references %s(%s)" ))))

(defn column-spec
  "Returns the column specification for the clojure.java.jdbc create-table fn."
  [column]
  (->> [(column-name column)
        (str (name (:type column)) (if-let [length (:length column)] (str "(" length ")")))
        (references-clause column)
        (if (:primary-key column) "primary key")
        (if (:unique column) "unique")
        (if (or (:primary-key column) (:not-null column)) "not null")]
       (remove nil?)))
