(ns datumbazo.io
  (:import java.io.Writer
           org.joda.time.DateTime
           org.joda.time.DateTimeZone
           org.postgresql.util.PGobject
           org.postgis.PGgeometry)
  (:require [clojure.instant :as i]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clj-time.coerce :refer [to-date-time to-sql-date to-timestamp]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer :all]
            [no.en.core :refer [parse-double parse-integer parse-long]]
            [sqlingvo.compiler :refer [SQLType]]
            [sqlingvo.expr :refer [parse-table]]
            geo.postgis))

(extend-type DateTime
  SQLType
  (sql-type [date-time]
    (to-timestamp date-time)))

(defn citext [s]
  (if s
    (doto (PGobject.)
      (.setValue (str s))
      (.setType "citext"))))

;; ENCODE

(defmulti encode-column
  (fn [column value] (:type column)))

(defmethod encode-column :citext [column value]
  (citext value))

(defmethod encode-column :date [column value]
  (to-sql-date value))

(defmethod encode-column :geometry [column value]
  (if value (PGgeometry. value)))

(defmethod encode-column :int4 [column value]
  (parse-integer value))

(defmethod encode-column :int8 [column value]
  (parse-long value))

(defmethod encode-column :serial [column value]
  (parse-integer value))

(defmethod encode-column :numeric [column value]
  (parse-double value))

(defmethod encode-column :text [column value]
  (if value (str value)))

(defmethod encode-column :timestamptz [column value]
  (to-timestamp value))

(defmethod encode-column :uuid [column value]
  (if value (java.util.UUID/fromString (str value))))

(defmethod encode-column :default [column value]
  value)

(defn- encode-columns
  "Encode the columns of `row` into database types."
  [columns row]
  (reduce (fn [row column]
            (cond
             ;; TODO: Howto insert/update raster?
             (= :raster (:type column))
             (dissoc row (:name column))
             (contains? row (:name column))
             (assoc row (:name column) (encode-column column (get row (:name column))))
             :else row))
          (select-keys row (map :name columns))
          columns))

(defn encode-row
  "Encode the columns of `row` into database types."
  [db table row]
  (let [table (parse-table table)]
    (encode-columns
     (meta/columns db :schema (:schema table) :table (:name table))
     row)))

(defn encode-rows
  "Encode the columns of `rows` into database types."
  [db table rows]
  (let [table (parse-table table)
        columns (meta/columns db :schema (:schema table) :table (:name table))]
    (map (partial encode-columns columns) rows)))

;; DECODE

(defmulti decode-pgobject
  (fn [pgobject] (keyword (.getType pgobject))))

(defmethod decode-pgobject :citext [pgobject]
  (.getValue pgobject))

(defmethod decode-pgobject :json [pgobject]
  (if-let [value (.getValue pgobject)]
    (json/read-str value :key-fn keyword)))

(defmethod decode-pgobject :default [pgobject]
  pgobject)

(defmulti decode-column class)

(defmethod decode-column PGgeometry [value]
  (.getGeometry value))

(defmethod decode-column PGobject [value]
  (decode-pgobject value))

(defn- decode-time
  "Deocde `time` using the configured EDN semantics."
  [time] (if time (edn/read-string {:readers *data-readers*} (prn-str time))))

(defmethod decode-column DateTime [value]
  (decode-time value))

(defmethod decode-column java.util.Date [value]
  (decode-time value))

(defmethod decode-column java.sql.Timestamp [value]
  (decode-time value))

(defmethod decode-column org.postgresql.jdbc2.AbstractJdbc2Array [array]
  (map decode-column (.getArray array)))

(defmethod decode-column :default [value]
  value)

(defn decode-row
  "Decode the columns of `row` into Clojure types."
  [row]
  (reduce
   #(update-in %1 [%2] decode-column)
   row (keys row)))

;; PRINT

(defmulti print-pgobject
  (fn [^PGobject o ^Writer w] (keyword (.getType o))))

(defmethod print-pgobject :default [^PGobject o ^Writer w]
  (print-method (str o) w))

;; PRINT-DUP

(defmethod print-dup DateTime
  [^DateTime d ^Writer w]
  (print-dup (.toDate d) w))

(defmethod print-dup PGobject
  [^PGobject o ^Writer w]
  (print-pgobject o w))


;; PRINT-METHOD

(defmethod print-method DateTime
  [^DateTime d ^Writer w]
  (print-method  (.toDate d) w))

(defmethod print-method PGobject
  [^PGobject o ^Writer w]
  (print-pgobject o w))

;; READ

(defn- construct-date-time
  [years months days hours minutes seconds nanoseconds offset-sign offset-hours offset-minutes]
  (-> (.getTimeInMillis
       (#'i/construct-calendar
        years months days
        hours minutes seconds nanoseconds
        offset-sign offset-hours offset-minutes))
      (DateTime.) (.withZone DateTimeZone/UTC)))

(def read-instant-date-time
  "To read an instant as an DateTime, bind *data-readers* to a map
with this var as the value for the 'inst key."
  (partial i/parse-timestamp (i/validated construct-date-time)))

(extend-protocol jdbc/IResultSetReadColumn
  org.postgresql.jdbc2.AbstractJdbc2Array
  (result-set-read-column [val rsmeta idx]
    (seq (.getArray val))))
