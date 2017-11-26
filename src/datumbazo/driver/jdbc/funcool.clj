(ns datumbazo.driver.jdbc.funcool
  (:require [clojure.string :as str]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]))

(defn- tx-strategy [driver & [opts]]
  (or (:strategy opts)
      (-> driver :connection meta :tx-strategy)
      jdbc/*default-tx-strategy*))

(defrecord Driver [connection]
  d/IConnection
  (-connect [driver opts]
    (->> (if-let [datasource (:datasource driver)]
           (jdbc/connection datasource)
           (jdbc/connection (util/format-url driver)))
         (assoc driver :connection)))
  (-connection [driver]
    (some-> connection proto/connection))
  (-disconnect [driver]
    (some-> connection .close)
    (assoc driver :connection nil))

  d/IExecute
  (-execute [driver sql opts]
    (d/row-count (jdbc/execute connection sql)))

  d/IFetch
  (-fetch [driver sql opts]
    (let [identifiers (or (:sql-keyword driver) str/lower-case)
          opts (merge {:identifiers identifiers} opts)]
      (jdbc/fetch connection sql opts)))

  d/IPrepareStatement
  (-prepare-statement [driver sql opts]
    (proto/prepared-statement sql (d/-connection driver) opts))

  d/ITransaction
  (-begin [driver opts]
    (->> (proto/begin! (tx-strategy driver) connection opts)
         (assoc driver :connection)))
  (-commit [driver opts]
    (->> (proto/commit! (tx-strategy driver) connection opts)
         (assoc driver :connection)))
  (-rollback [driver opts]
    (jdbc/set-rollback! connection)
    (proto/rollback! (tx-strategy driver) connection opts)
    driver))

(defmethod d/find-driver 'jdbc.core
  [db & [opts]]
  (map->Driver db))

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

  clojure.lang.BigInt
  (as-sql-type [big-int conn]
    (long big-int))
  (set-stmt-parameter! [big-int conn stmt index]
    (.setObject stmt index (proto/as-sql-type big-int conn)))

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
