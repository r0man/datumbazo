(ns datumbazo.driver.jdbc.core
  (:require [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto])
  (:import [java.sql Connection PreparedStatement]))

(defrecord Driver [config connection]
  d/Connectable
  (-connect [this db opts]
    (->> (or connection
             (jdbc/connection
              (or (:datasource db)
                  (util/format-url db))))
         (assoc this :connection)))

  (-connection [this db]
    (some-> connection proto/connection))

  (-disconnect [this db]
    (some-> connection .close)
    (assoc this :connection nil))

  d/Executeable
  (-execute-all [this db sql opts]
    (jdbc/fetch connection sql (merge config opts)))
  (-execute-one [this db sql opts]
    (d/row-count (jdbc/execute connection sql (merge config opts))))


  d/Preparable
  (-prepare [this db sql opts]
    (proto/prepared-statement sql (d/-connection this db) (merge config opts)))

  d/Transactable
  (-transact [this db f opts]
    (jdbc/atomic-apply connection (fn [connection]
                                    (when (true? (:rollback-only opts))
                                      (jdbc/set-rollback! connection)
                                      (f (assoc-in db [:driver :connection] connection))))
                       (merge config opts))))

(defmethod d/driver :jdbc.core
  [db & [opts]]
  (map->Driver {:config (:jdbc.core db)}))

(extend-protocol proto/ISQLResultSetReadColumn
  java.sql.Date
  (from-sql-type [val conn metadata index]
    (io/decode-date val))

  java.sql.Timestamp
  (from-sql-type [val conn metadata index]
    (io/decode-date val)))

(extend-protocol proto/ISQLType
  clojure.lang.BigInt
  (as-sql-type [big-int conn]
    big-int)
  (set-stmt-parameter! [big-int conn ^PreparedStatement stmt index]
    (.setObject stmt index big-int))

  java.util.Date
  (as-sql-type [date conn]
    (io/encode-timestamp date))
  ;; TODO: The `set-stmt-parameter!` on Object doesn't work. Maybe split protocols.
  (set-stmt-parameter! [date conn ^PreparedStatement stmt index]
    (.setObject stmt index (proto/as-sql-type date conn))))

;; Joda Time

(util/with-library-loaded :joda-time

  (extend-protocol proto/ISQLType
    org.joda.time.DateTime
    (as-sql-type [date-time conn]
      (io/encode-timestamp date-time))
    (set-stmt-parameter! [date-time conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type date-time conn)))))

;; PostgreSQL

(util/with-library-loaded :postgresql
  (extend-protocol proto/ISQLResultSetReadColumn
    org.postgresql.jdbc.PgArray
    (from-sql-type [val conn metadata index]
      (io/decode-array val))

    org.postgresql.util.PGobject
    (from-sql-type [val conn metadata index]
      (io/decode-pgobject val))))

;; PostGIS

(util/with-library-loaded :postgis
  (extend-protocol proto/ISQLResultSetReadColumn
    org.postgis.PGgeo
    (from-sql-type [val conn metadata index]
      (io/decode-pggeometry val)))

  (extend-protocol proto/ISQLType
    org.postgis.LineString
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))

    org.postgis.LinearRing
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))

    org.postgis.MultiLineString
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))

    org.postgis.MultiPoint
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))

    org.postgis.MultiPolygon
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))

    org.postgis.Point
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))

    org.postgis.Polygon
    (as-sql-type [geometry conn]
      (io/encode-pggeometry geometry))
    (set-stmt-parameter! [^org.postgis.Polygon geometry ^Connection conn ^PreparedStatement stmt index]
      (.setObject stmt index (proto/as-sql-type geometry conn)))))
