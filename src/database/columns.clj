(ns database.columns
  (:refer-clojure :exclude (replace))
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (join split replace)]
        [database.connection :only (naming-strategy)]))

(defprotocol IColumn
  (column-name [column]
    "Returns the name of the column as a string."))

(defrecord Column [name type length default native? not-null? primary-key? references unique?])

(defn column?
  "Returns true if arg is a column, otherwise false."
  [arg] (instance? Column arg))

(defn column-identifier
  "Returns the column identifier. Given a string, return it as-is.
  Given a keyword, return it as a string using the current naming
  strategy." [column] ((:fields (naming-strategy)) (column-name column)))

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
    :not-null? (or (:not-null? attributes) (:primary-key? attributes))))

(defn remove-serial-columns
  "Remove the serial columns from `record` if their value is nil."
  [table record]
  (->> (remove #(and (= :serial (:type %1))
                     (nil? (get record (keyword (column-name %1)))))
               (vals (:columns table)))
       (map (comp keyword column-name))
       (select-keys record)))

(defn unique-column?
  "Returns true if `column` is a primary key or is unique, otherwise
  false." [column] (or (:primary-key? column) (:unique? column)))

;; SQL CLAUSE FNS

(defn default-clause
  "Returns the default clause for column."
  [column] (if (:default column) (str "default " (:default column))))

(defn not-null-clause
  "Returns the not null clause for column."
  [column] (if (or (:primary-key? column) (:not-null? column)) "not null"))

(defn primary-key-clause
  "Returns the primary key for column."
  [column] (if (:primary-key? column) "primary key"))

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

(defn drop-column-statement
  "Returns the drop column SQL statement."
  [table column]
  (format "ALTER TABLE %s DROP COLUMN %s"
          (jdbc/as-identifier (:name table))
          (column-identifier column)))

(extend-type Column
  IColumn
  (column-name [column]
    (column-name (:name column))))

(extend-type String
  IColumn
  (column-name [string]
    string))

(extend-type clojure.lang.IPersistentMap
  IColumn
  (column-name [column]
    (if-let [name (:name column)]
      (column-name name)
      (throw (IllegalArgumentException. (format "Not a column: %s" (prn-str column)))))))

(extend-type clojure.lang.Keyword
  IColumn
  (column-name [keyword]
    (name keyword)))

(extend-type clojure.lang.Symbol
  IColumn
  (column-name [symbol]
    (str symbol)))
