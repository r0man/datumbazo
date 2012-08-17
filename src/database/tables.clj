(ns database.tables
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? split upper-case]]
            [database.meta :refer :all]
            [database.connection :refer [*naming-strategy*]]
            [database.protocol :refer [as-keyword]])
  (:import database.protocol.Nameable))

(defn column-prefix [table column]
  (keyword (str (name (:name table)) "-" (name (:name column)))))

(defn default-columns
  "Returns the default columns of `table`."
  [table] (vals (apply dissoc (:columns table) (:exclude (:fields table)))))

(defn select-columns
  "Select all columns of `table` which are in `selection`."
  [table selection]
  (let [selection (set (map #(as-keyword %1) selection))]
    (filter #(contains? selection (as-keyword %1))
            (vals (apply dissoc (:columns table) (:exclude (:fields table)))))))

(defn primary-key-columns
  "Returns all primary key columns of `table`."
  [table] (filter :primary-key? (vals (:columns table))))

(defn unique-columns
  "Returns all unique columns of `table`."
  [table] (filter :unique? (vals (:columns table))))

(defn key-columns
  "Returns the key columns of `table` for `record`."
  [table record]
  (let [key-set (set (keys record))]
    (filter #(contains? key-set (:name %1))
            (concat (primary-key-columns table)
                    (unique-columns table)))))
