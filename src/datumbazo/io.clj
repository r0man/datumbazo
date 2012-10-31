(ns datumbazo.io
  (:import java.io.Writer
           org.postgresql.util.PGobject)
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer :all]))

;; ENCODE

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

(defn encode-row
  "Encode the columns of `row` into database types."
  [table row]
  (let [columns (meta/columns (jdbc/connection) :table table)]
    (reduce (fn [row column]
              (if (contains? row (:name column))
                (assoc row (:name column) (encode-column column (get row (:name column))))
                row))
            (select-keys row (map :name columns))
            columns)))

;; DECODE

(defmulti decode-pg-object
  (fn [pg-object] (keyword (.getType pg-object))))

(defmethod decode-pg-object :default [pg-object]
  (.getValue pg-object))

(defmulti decode-column class)

(defmethod decode-column PGobject [value]
  (decode-pg-object value))

(defmethod decode-column :default [value]
  value)

(defn decode-row
  "Decode the columns of `row` into Clojure types."
  [row]
  (reduce
   #(update-in %1 [%2] decode-column)
   row (keys row)))

;; PRINT

(defmulti print-method-pg-object
  (fn [^PGobject pg-object ^Writer w]
    (keyword (.getType pg-object))))

(defmethod print-method-pg-object :default [^PGobject pg-object ^Writer w]
  (print-method (.getValue pg-object) w))

(defmethod print-method PGobject [^PGobject pg-object ^Writer w]
  (print-method-pg-object pg-object w))

(defmethod print-dup PGobject [o w]
  (print-method o w))