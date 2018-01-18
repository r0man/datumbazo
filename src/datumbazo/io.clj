(ns datumbazo.io
  (:require [clj-time.coerce :refer [to-date to-sql-date to-timestamp]]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.instant :as i]
            [datumbazo.meta :as meta]
            [no.en.core :refer [parse-double parse-integer parse-long]]
            [sqlingvo.expr :refer [parse-table]])
  (:import java.io.Writer
           [org.joda.time DateTime DateTimeZone]
           org.postgis.PGgeometry
           org.postgresql.util.PGobject))

(defn citext [s]
  (if s
    (doto (PGobject.)
      (.setValue (str s))
      (.setType "citext"))))

(defn jsonb
  "Convert `x` into a JSONB type."
  [x]
  (doto (PGobject.)
    (.setValue (json/json-str x))
    (.setType "jsonb")))

;; ENCODE

(defn encode-pggeometry
  "Encode a PGgeometry column."
  [x]
  (PGgeometry. x))

(defn encode-timestamp
  "Encode a timestamp column."
  [x]
  (to-timestamp x))

(defmulti encode-column
  (fn [column value] (:type column)))

(defmethod encode-column :citext [column value]
  (citext value))

(defmethod encode-column :date [column value]
  (to-sql-date value))

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

(defn- find-encode-columns [db table]
  (meta/columns db {:schema (or (:schema table) "public") :table (:name table)}))

(defn encode-row
  "Encode the columns of `row` into database types."
  [db table row]
  (let [table (parse-table table)]
    (encode-columns (find-encode-columns db table) row)))

(defn encode-rows
  "Encode the columns of `rows` into database types."
  [db table rows]
  (let [table (parse-table table)
        columns (find-encode-columns db table)]
    (map (partial encode-columns columns) rows)))

;; DECODE

(defmulti decode-column class)

(comment

  ;; TODO: This unfortunately does not work with multi dimensional
  ;; arrays. It throws an ArrayIndexOutOfBoundsException sometimes.
  (defn decode-array
    "Decode an array column."
    [x]
    (with-open [result-set (.getResultSet x)]
      (mapv (comp decode-column :value)
            (doall (resultset-seq result-set))))))

(defn decode-array
  "Decode an array column."
  [x]
  (mapv decode-column (.getArray x)))

(defmacro define-array-decoders [dimensions & classes]
  `(do ~@(for [class# classes, dimension# (range 1 dimensions)
               :let [class-name# (str (apply str (repeat dimension# "[")) "L" class# ";")]]
           `(defmethod decode-column (Class/forName ~class-name#) [~'array]
              (mapv decode-column ~'array)))))

(define-array-decoders 10
  java.lang.Float
  java.lang.Integer
  java.lang.Long
  java.lang.String
  java.math.BigDecimal
  java.util.UUID)

(defmethod decode-column org.postgresql.jdbc.PgArray [array]
  (decode-array array))

(defn decode-pggeometry
  "Decode a PGgeometry column."
  [x]
  (.getGeometry x))

(defn decode-date
  "Decode a date column."
  [x]
  (to-date x))

(defmulti decode-pgobject
  (fn [pgobject] (keyword (.getType pgobject))))

(defn- decode-json [pgobject]
  (when-let [value (.getValue pgobject)]
    (json/read-str value :key-fn keyword)))

(defmethod decode-pgobject :json [pgobject]
  (decode-json pgobject))

(defmethod decode-pgobject :jsonb [pgobject]
  (decode-json pgobject))

(defmethod decode-pgobject :default [pgobject]
  (.getValue pgobject))

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
