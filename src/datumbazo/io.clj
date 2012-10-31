(ns datumbazo.io
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer :all]))

(defn- encode-integer [integer]
  (cond
   (integer? integer)
   integer
   (string? integer)
   (Integer/parseInt integer)
   (nil? integer)
   nil
   :else (illegal-argument-exception "Can't encode integer: %s" integer)))

(defmulti encode-column
  (fn [column value] (:type column)))

(defmethod encode-column :int4 [column value]
  (encode-integer value))

(defmethod encode-column :serial [column value]
  (encode-integer value))

(defmethod encode-column :timestamptz [column value]
  value)

(defmethod encode-column :default [column value]
  value)

(defmulti decode-column class)

(defmethod decode-column :citext [column value]
  (println "CITEXT")
  (.getValue value))

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
