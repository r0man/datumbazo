(ns datumbazo.driver.funcool
  (:require [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [datumbazo.driver.core :refer :all]
            [datumbazo.io :as io]
            [sqlingvo.compiler :refer [compile-stmt]]
            [clojure.string :as str])
  (:import sqlingvo.db.Database))

(defmethod apply-transaction 'jdbc.core [db f & [opts]]
  (jdbc/atomic-apply
   (:connection db)
   (fn [connection]
     (f (assoc db :connection connection)))
   opts))

(defmethod close-db 'jdbc.core [db]
  (.close (:connection db)))

(defmethod fetch 'jdbc.core [db sql & [opts]]
  (let [opts (merge {:identifiers (or (:sql-keyword db) str/lower-case)} opts)]
    (jdbc/fetch (:connection db) sql opts)))

(defmethod execute 'jdbc.core [db sql & [opts]]
  (row-count (jdbc/execute (:connection db) sql)))

(defmethod open-db 'jdbc.core [db]
  (assoc db :connection (jdbc/connection (into {} db))))

(extend-protocol proto/ISQLResultSetReadColumn

  org.postgresql.jdbc.PgArray
  (from-sql-type [val conn metadata index]
    (io/decode-array val))

  org.postgresql.util.PGobject
  (from-sql-type [val conn metadata index]
    (io/decode-pgobject val))

  org.postgis.PGgeometry
  (from-sql-type [val conn metadata index]
    (io/decode-pggeometry val))

  java.sql.Date
  (from-sql-type [val conn metadata index]
    (io/decode-date val))

  java.sql.Timestamp
  (from-sql-type [val conn metadata index]
    (io/decode-date val)))

(extend-protocol proto/ISQLType

  java.util.Date
  (as-sql-type [date conn]
    (io/encode-timestamp date))
  ;; TODO: The `set-stmt-parameter!` on Object doesn't work. Maybe split protocols.
  (set-stmt-parameter! [date conn stmt index]
    (.setObject stmt index (proto/as-sql-type date conn)))

  org.joda.time.DateTime
  (as-sql-type [date-time conn]
    (io/encode-timestamp date-time))
  (set-stmt-parameter! [date-time conn stmt index]
    (.setObject stmt index (proto/as-sql-type date-time conn)))

  org.postgis.LineString
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn)))

  org.postgis.LinearRing
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn)))

  org.postgis.MultiLineString
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn)))

  org.postgis.MultiPoint
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn)))

  org.postgis.MultiPolygon
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn)))

  org.postgis.Point
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn)))

  org.postgis.Polygon
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))
  (set-stmt-parameter! [geometry conn stmt index]
    (.setObject stmt index (proto/as-sql-type geometry conn))))
