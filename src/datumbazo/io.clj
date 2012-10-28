(ns datumbazo.io
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.meta :as meta]))

(defmulti encode-column
  (fn [column value] (:type column)))

(defmethod encode-column :int4 [column value]
  value)

(defmethod encode-column :serial [column value]
  value)

(defmethod encode-column :timestamptz [column value]
  value)

(defmethod encode-column :default [column value]
  value)

(defmulti decode-column
  (fn [column value] (:type column)))

(defmethod decode-column :default [column value]
  value)

(defn transform-row
  "Transform the columns of `row` via `transform-fn`."
  [table row transform-fn]
  (let [columns (meta/columns (jdbc/connection) :table table)]
    (reduce (fn [row column]
              (if (contains? row (:name column))
                (assoc row (:name column) (transform-fn column (get row (:name column))))
                row))
            (select-keys row (map :name columns))
            columns)))

(defn encode-row
  "Encode the columns of `row` into database types."
  [table row]
  (transform-row table row encode-column))

(defn decode-row
  "Decode the columns of `row` into Clojure types."
  [table row]
  (transform-row table row decode-column))
