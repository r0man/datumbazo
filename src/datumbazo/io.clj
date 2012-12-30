(ns datumbazo.io
  (:import java.io.Writer
           org.joda.time.DateTime
           org.postgis.Geometry
           org.postgis.PGgeometry
           org.postgresql.util.PGobject)
  (:require [clojure.instant :as i]
            [clojure.java.jdbc :as jdbc]
            [clj-time.coerce :refer [to-date-time to-sql-date to-timestamp]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer :all]
            [inflections.util :refer [parse-double parse-integer]]
            [sqlingvo.compiler :refer [SQLType]]
            [sqlingvo.util :refer [parse-table]]))

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
  (if value
    (doto (PGobject.)
      (.setValue (str value))
      (.setType "citext"))))

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

(defmethod decode-pg-object :citext [pg-object]
  (.getValue pg-object))

(defmethod decode-pg-object :default [pg-object]
  pg-object)

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

(defn- print-wkt
  "Print a org.postgis.PGgeometry in WKT format."
  [^PGgeometry g ^java.io.Writer w]
  (.write w "#wkt \"")
  (.write w (str g))
  (.write w "\""))

;; PRINT-DUP

(defmethod print-dup DateTime
  [^DateTime d ^Writer w]
  (print-dup (.toDate d) w))

(defmethod print-dup Geometry
  [^Geometry g ^java.io.Writer w]
  (print-wkt g w))

(defmethod print-dup PGgeometry
  [^PGgeometry g ^java.io.Writer w]
  (print-wkt g w))

;; PRINT-METHOD

(defmethod print-method DateTime
  [^DateTime d ^Writer w]
  (print-method  (.toDate d) w))

(defmethod print-method Geometry
  [^Geometry g ^java.io.Writer w]
  (print-wkt g w))

(defmethod print-method PGgeometry
  [^PGgeometry g ^java.io.Writer w]
  (print-wkt g w))

;; READ

(defn- construct-date-time [years months days hours minutes seconds nanoseconds offset-sign offset-hours offset-minutes]
  (DateTime. (.getTimeInMillis
              (#'i/construct-calendar
               years months days
               hours minutes seconds 0
               offset-sign offset-hours offset-minutes))))

(defn read-wkt
  "Read a geometry from `s` in WKT format."
  [s] (PGgeometry. (PGgeometry/geomFromString s)))

(def read-instant-date-time
  "To read an instant as an DateTime, bind *data-readers* to a map
with this var as the value for the 'inst key."
  (partial i/parse-timestamp (i/validated construct-date-time)))
