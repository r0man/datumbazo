(ns datumbazo.driver.funcool
  (:require [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [datumbazo.driver.core :refer :all]
            [datumbazo.io :as io]
            [sqlingvo.compiler :refer [compile-stmt]])
  (:import sqlingvo.db.Database))

(defmethod apply-transaction 'jdbc.core [db f & [opts]]
  (jdbc/atomic-apply
   (:connection db)
   (fn [connection]
     (f (assoc db :connection connection)))
   opts))

(defmethod close-db 'jdbc.core [db]
  (.close (:connection db)))

(defmethod eval-db* 'jdbc.core
  [{:keys [db] :as ast} & [opts]]
  (let [conn (:connection db)
        sql (compile-stmt ast)]
    (assert conn "No database connection!")
    (case (:op ast)
      :delete
      (if (:returning ast)
        (jdbc/fetch conn sql)
        (row-count (jdbc/execute conn sql)))
      :except
      (jdbc/fetch conn sql)
      :insert
      (if (:returning ast)
        (jdbc/fetch conn sql)
        (row-count (jdbc/execute conn sql)))
      :intersect
      (jdbc/fetch conn sql)
      :select
      (jdbc/fetch conn sql)
      :union
      (jdbc/fetch conn sql)
      :update
      (if (:returning ast)
        (jdbc/fetch conn sql)
        (row-count (jdbc/execute conn sql)))
      (row-count (jdbc/execute conn sql)))))

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

  org.joda.time.DateTime
  (as-sql-type [date-time conn]
    (io/encode-timestamp date-time))

  org.postgis.LineString
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))

  org.postgis.LinearRing
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))

  org.postgis.MultiLineString
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))

  org.postgis.MultiPoint
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))

  org.postgis.MultiPolygon
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))

  org.postgis.Point
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry))

  org.postgis.Polygon
  (as-sql-type [geometry conn]
    (io/encode-pggeometry geometry)))
