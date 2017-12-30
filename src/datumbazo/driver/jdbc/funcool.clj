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

  d/IExecute
  (-execute [this db sql opts]
    (d/row-count (jdbc/execute connection sql)))

  d/IFetch
  (-fetch [this db sql opts]
    (let [identifiers (or (:sql-keyword db) str/lower-case)
          opts (merge {:identifiers identifiers} opts)]
      (jdbc/fetch connection sql opts)))

  d/IPrepareStatement
  (-prepare-statement [this db sql opts]
    (proto/prepared-statement sql (d/-connection this db) opts))

  d/ITransaction
  (-begin [this db opts]
    (->> (proto/begin! (tx-strategy this) connection opts)
         (assoc this :connection)))

  (-commit [this db opts]
    (->> (proto/commit! (tx-strategy this) connection opts)
         (assoc this :connection)))

  (-rollback [this db opts]
    (jdbc/set-rollback! connection)
    (proto/rollback! (tx-strategy this) connection opts)
    this))

(defmethod d/find-driver 'jdbc.core
  [db & [opts]]
  (map->Driver {}))

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
