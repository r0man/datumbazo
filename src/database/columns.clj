(ns database.columns
  (:refer-clojure :exclude (replace))
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (split replace)]
        [inflections.core :only (dasherize)]))

(defrecord Column [name type native? length default not-null primary-key references unique])

(defn column-identifier
  "Returns the column identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [column] (jdbc/as-identifier (:name column)))

(defn column-symbol
  "Returns the name of the column as a symbol with all underscores in
  the name replaced by dashes."
  [column] (symbol (name (dasherize (:name column)))))

(defn column-keyword
  "Returns the name of the column as a keyword with all underscores in
  the name replaced by dashes."
  [column] (keyword (column-symbol column)))

(defn column-type-name
  "Returns the type of the column as string."
  [column]
  (let [type (:type column)]
    (if (string? type) type (replace (name type) #"-+" " "))))

(defn make-column
  "Make a new database column."
  [name type & {:as attributes}]
  (assoc (map->Column (or attributes {}))
    :name (keyword name)
    :type (keyword (if (sequential? type) (first type) type))
    :native? (if (sequential? type) false true)
    :not-null (or (:not-null attributes) (:primary-key attributes))))

(defn- references-clause [column]
  (if-let [reference (:references column)]
    (->> (split (replace (str reference) #":" "") #"/")
         (map column-identifier)
         (apply format "references %s(%s)" ))))

(defn column-spec
  "Returns the column specification for the clojure.java.jdbc create-table fn."
  [column]
  (->> [(column-identifier column)
        (str (column-type-name column) (if-let [length (:length column)] (str "(" length ")")))
        (references-clause column)
        (if (:primary-key column) "primary key")
        (if (:unique column) "unique")
        (if (or (:primary-key column) (:not-null column)) "not null")]
       (remove nil?)))
