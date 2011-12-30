(ns database.columns
  (:refer-clojure :exclude (replace))
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (join split replace)]
        [inflections.core :only (dasherize)]))

(defrecord Column [name type length default native? not-null? primary-key references unique?])

(defn column?
  "Returns true if arg is a column, otherwise false."
  [arg] (instance? Column arg))

(defn column-name
  "Returns the name of the column."
  [column] (if (column? column) (:name column) column))

(defn column-identifier
  "Returns the column identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [column] (jdbc/as-identifier (column-name column)))

(defn column-symbol
  "Returns the name of the column as a symbol with all underscores in
  the name replaced by dashes."
  [column] (symbol (name (dasherize (column-name column)))))

(defn column-keyword
  "Returns the name of the column as a keyword with all underscores in
  the name replaced by dashes."
  [column] (keyword (column-symbol column)))

(defn column-type-name
  "Returns the type of the column as string."
  [column]
  (let [type (:type column)]
    (str (if (string? type)
           type (replace (name type) #"-+" " "))
         (if-let [length (:length column)] (str "(" length ")")))))

(defn make-column
  "Make a new database column."
  [name type & {:as attributes}]
  (assoc (map->Column (or attributes {}))
    :name (keyword name)
    :type (keyword (if (sequential? type) (first type) type))
    :native? (if (sequential? type) false true)
    :not-null? (or (:not-null? attributes) (:primary-key attributes))))

(defn select-columns
  "Select columns of table by keywords."
  [table columns]
  (let [columns (set columns)]
    (filter #(contains? columns (column-keyword %1)) (:columns table))))

;; SQL CLAUSE FNS

(defn default-clause
  "Returns the default clause for column."
  [column] (if (:default column) (str "default " (:default column))))

(defn not-null-clause
  "Returns the not null clause for column."
  [column] (if (or (:primary-key column) (:not-null? column)) "not null"))

(defn primary-key-clause
  "Returns the primary key for column."
  [column] (if (:primary-key column) "primary key"))

(defn references-clause
  "Returns the unique clause for column."
  [column]
  (if-let [reference (:references column)]
    (->> (split (replace (str reference) #":" "") #"/")
         (map column-identifier)
         (apply format "references %s(%s)" ))))

(defn unique-clause
  "Returns the unique clause for column."
  [column] (if (:unique? column) "unique"))

(defn column-spec
  "Returns the column specification for the clojure.java.jdbc create-table fn."
  [column]
  (->> [(column-identifier column)
        (column-type-name column)
        (references-clause column)
        (primary-key-clause column)
        (default-clause column)
        (not-null-clause column)
        (unique-clause column)]
       (remove nil?)))

(defn add-column-statement
  "Returns the add column SQL statement."
  [table column]
  (->> [(str "ALTER TABLE " (jdbc/as-identifier (:name table)))
        (str "ADD COLUMN " (column-identifier column))
        (column-type-name column)
        (default-clause column)
        (not-null-clause column)
        (unique-clause column)]
       (join " ")))
