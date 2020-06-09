(ns datumbazo.driver.clojure.java.jdbc
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util])
  (:import java.sql.Connection))

(defrecord Driver [config ^Connection connection]
  d/Connectable
  (-connect [this db opts]
    (->> (or connection
             (jdbc/get-connection
              (if (:datasource db)
                (select-keys db [:datasource])
                (util/format-url db))))
         (assoc this :connection)))

  (-connection [this db]
    connection)

  (-disconnect [this db]
    (some-> connection .close)
    (assoc this :connection nil))

  d/Executeable
  (-execute-all [this db sql opts]
    (jdbc/query this sql (merge config opts)))
  (-execute-one [this db sql opts]
    (d/row-count (jdbc/execute! this sql (merge config opts))))


  d/Preparable
  (-prepare [this db sql opts]
    (let [prepared (jdbc/prepare-statement connection (first sql) (merge config opts))]
      (dorun (map-indexed (fn [i v] (jdbc/set-parameter v prepared (inc i))) (rest sql)))
      prepared))

  d/Transactable
  (-transact [this db f opts]
    (jdbc/db-transaction* this (fn [driver]
                                 (when (true? (:rollback-only opts))
                                   (jdbc/db-set-rollback-only! driver))
                                 (f (assoc db :driver driver)))
                          (merge config opts))))

(defmethod d/driver :clojure.java.jdbc
  [db & [opts]]
  (map->Driver {:config (:clojure.java.jdbc db)}))

(extend-protocol jdbc/IResultSetReadColumn
  java.sql.Date
  (result-set-read-column [val rsmeta idx]
    (io/decode-date val))

  java.sql.Timestamp
  (result-set-read-column [val rsmeta idx]
    (io/decode-date val)))

(extend-protocol jdbc/ISQLValue
  clojure.lang.BigInt
  (sql-value [big-int]
    big-int)

  java.util.Date
  (sql-value [date]
    (io/encode-timestamp date)))

;; Joda Time

(util/with-library-loaded :joda-time

  (extend-protocol jdbc/ISQLValue
    org.joda.time.DateTime
    (sql-value [date-time]
      (io/encode-timestamp date-time))))

;; PostgreSQL

(util/with-library-loaded :postgresql

  (extend-protocol jdbc/IResultSetReadColumn
    org.postgresql.jdbc.PgArray
    (result-set-read-column [val rsmeta idx]
      (io/decode-array val))

    org.postgresql.util.PGobject
    (result-set-read-column [val rsmeta idx]
      (io/decode-pgobject val))))

;; PostGIS

(util/with-library-loaded :postgis

  (extend-protocol jdbc/IResultSetReadColumn
    org.postgis.PGgeo
    (result-set-read-column [val rsmeta idx]
      (io/decode-pggeometry val)))

  (extend-protocol jdbc/ISQLValue
    org.postgis.LineString
    (sql-value [geometry]
      (io/encode-pggeometry geometry))

    org.postgis.LinearRing
    (sql-value [geometry]
      (io/encode-pggeometry geometry))

    org.postgis.MultiLineString
    (sql-value [geometry]
      (io/encode-pggeometry geometry))

    org.postgis.MultiPoint
    (sql-value [geometry]
      (io/encode-pggeometry geometry))

    org.postgis.MultiPolygon
    (sql-value [geometry]
      (io/encode-pggeometry geometry))

    org.postgis.Point
    (sql-value [geometry]
      (io/encode-pggeometry geometry))

    org.postgis.Polygon
    (sql-value [geometry]
      (io/encode-pggeometry geometry))))
