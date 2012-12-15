(ns datumbazo.io
  (:import java.io.Writer
           org.joda.time.DateTime
           org.postgis.Geometry
           org.postgis.PGgeometry
           org.postgresql.util.PGobject)
  (:require [clojure.java.jdbc :as jdbc]
            [clj-time.coerce :refer [to-date-time to-sql-date to-timestamp]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer :all]
            [inflections.util :refer [parse-double parse-integer]]
            [sqlingvo.compiler :refer [SQLType]]))

(extend-type DateTime
  SQLType
  (sql-type [date-time]
    (to-timestamp date-time)))

;; ENCODE

(defmulti encode-column
  (fn [column value] (:type column)))

(defmethod encode-column :date [column value]
  (to-sql-date value))

(defmethod encode-column :int4 [column value]
  (parse-integer value))

(defmethod encode-column :serial [column value]
  (parse-integer value))

(defmethod encode-column :numeric [column value]
  (parse-double value))

(defmethod encode-column :citext [column value]
  (doto (PGobject.)
    (.setValue value)
    (.setType "citext")))

(defmethod encode-column :timestamptz [column value]
  (to-timestamp value))

(defmethod encode-column :default [column value]
  value)

(defn encode-row
  "Encode the columns of `row` into database types."
  [table row]
  (let [table (parse-table table)
        columns (meta/columns (jdbc/connection) :schema (:schema table) :table (:name table))]
    (reduce (fn [row column]
              (if (contains? row (:name column))
                (assoc row (:name column) (encode-column column (get row (:name column))))
                row))
            (select-keys row (map :name columns))
            columns)))

(defn encode-rows
  "Encode the columns of `rows` into database types."
  [table rows]
  (let [table (parse-table table)
        columns (meta/columns (jdbc/connection) :schema (:schema table) :table (:name table))]
    (map #(reduce (fn [row column]
                    (if (contains? row (:name column))
                      (assoc row (:name column) (encode-column column (get row (:name column))))
                      row))
                  (select-keys %1 (map :name columns))
                  columns)
         rows)))

;; DECODE

(defmulti decode-pg-object
  (fn [pg-object] (keyword (.getType pg-object))))

(defmethod decode-pg-object :default [pg-object]
  (.getValue pg-object))

(defmulti decode-column class)

(defmethod decode-column PGobject [value]
  (decode-pg-object value))

(defmethod decode-column java.util.Date [value]
  (to-date-time value))

(defmethod decode-column :default [value]
  value)

(defn decode-row
  "Decode the columns of `row` into Clojure types."
  [row]
  (reduce
   #(update-in %1 [%2] decode-column)
   row (keys row)))

;; PRINT

(defmulti print-pg-object
  (fn [^PGobject pg-object ^Writer w]
    (keyword (.getType pg-object))))

(defmethod print-pg-object :default [^PGobject pg-object ^Writer w]
  (print-method (.getValue pg-object) w))

(defn- print-wkt
  "Print a org.postgis.PGgeometry in WKT format."
  [^PGgeometry g ^java.io.Writer w]
  (.write w "#wkt \"")
  (.write w (str g))
  (.write w "\""))

(defmethod print-method DateTime
  [^DateTime d ^Writer w]
  (print-method (java.util.Date. (.getMillis d)) w))

(defmethod print-dup DateTime
  [^DateTime d ^Writer w]
  (print-dup (java.util.Date. (.getMillis d)) w))

(defmethod print-method Geometry
  [^Geometry g ^java.io.Writer w]
  (print-wkt g w))

(defmethod print-dup Geometry
  [^Geometry g ^java.io.Writer w]
  (print-wkt g w))

(defmethod print-method PGgeometry
  [^PGgeometry g ^java.io.Writer w]
  (print-wkt g w))

(defmethod print-dup PGgeometry
  [^PGgeometry g ^java.io.Writer w]
  (print-wkt g w))

(defmethod print-method PGobject [^PGobject o ^Writer w]
  (print-pg-object o w))

(defmethod print-dup PGobject [^PGobject o ^Writer w]
  (print-pg-object o w))

;; READ

(defn read-wkt
  "Read a geometry from `s` in WKT format."
  [s] (PGgeometry/geomFromString s))
