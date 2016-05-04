(ns datumbazo.driver.funcool
  (:require [clojure.string :as str]
            [datumbazo.db :refer [format-url]]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]))

(defrecord Driver [connection])

(defmethod d/find-driver 'jdbc.core
  [db & [opts]]
  (map->Driver db))

(defn- connected?
  "Return true if `driver` is connected, otherwise false."
  [driver]
  (:connection driver))

(defn- tx-strategy [driver & [opts]]
  (or (:strategy opts)
      (-> driver :connection meta :tx-strategy)
      jdbc/*default-tx-strategy*))

(defn- begin
  "Begin a new database transaction."
  [driver & [opts]]
  {:pre [(connected? driver)]}
  (->> (proto/begin!
        (tx-strategy driver)
        (:connection driver)
        opts)
       (assoc driver :connection)))

(defn- connect
  "Connect to the database."
  [driver & [opts]]
  (->> (if-let [datasource (:datasource driver)]
         (jdbc/connection datasource)
         (jdbc/connection (format-url driver)))
       (assoc driver :connection)))

(defn- commit
  "Commit the currenbt database transaction."
  [driver & [opts]]
  {:pre [(connected? driver)]}
  (let [connection (:connection driver)
        tx-strategy (tx-strategy driver)]
    (assoc driver :connection (proto/commit! tx-strategy connection opts))))

(defn- connection
  "Return the current database connection."
  [driver]
  (some-> driver :connection proto/connection))

(defn- disconnect
  "Disconnect from the database."
  [driver & [opts]]
  {:pre [(connected? driver)]}
  (when-let [connection (d/-connection driver)]
    (.close connection))
  (assoc driver :connection nil))

(defn- fetch
  "Query the database and fetch the result."
  [driver sql & [opts]]
  {:pre [(connected? driver)]}
  (let [identifiers (or (:sql-keyword driver) str/lower-case)
        opts (merge {:identifiers identifiers} opts)]
    (try
      (jdbc/fetch (:connection driver) sql opts)
      (catch Exception e
        (util/throw-sql-ex-info e sql)))))

(defn- execute
  "Execute a SQL statement against the database."
  [driver sql & [opts]]
  {:pre [(connected? driver)]}
  (try (d/row-count (jdbc/execute (:connection driver) sql))
       (catch Exception e
         (util/throw-sql-ex-info e sql))))

(defn- prepare-statement
  "Return a prepared statement for the `sql` statement."
  [driver sql & [opts]]
  {:pre [(connected? driver)]}
  (proto/prepared-statement sql (d/-connection driver) opts))

(defn- rollback!
  "Mark the current database transaction for rollback."
  [driver & [opts]]
  {:pre [(connected? driver)]}
  (jdbc/set-rollback! (:connection driver))
  driver)

(extend-protocol d/IConnection
  Driver
  (-connect [driver opts]
    (connect driver opts))
  (-connection [driver]
    (connection driver))
  (-disconnect [driver]
    (disconnect driver)))

(extend-protocol d/IExecute
  Driver
  (-execute [driver sql opts]
    (execute driver sql opts)))

(extend-protocol d/IFetch
  Driver
  (-fetch [driver sql opts]
    (fetch driver sql opts)))

(extend-protocol d/IPrepareStatement
  Driver
  (-prepare-statement [driver sql opts]
    (prepare-statement driver sql opts)))

(extend-protocol d/ITransaction
  Driver
  (-begin [driver opts]
    (begin driver opts))
  (-commit [driver opts]
    (commit driver opts))
  (-rollback [driver opts]
    (rollback! driver opts)))

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
