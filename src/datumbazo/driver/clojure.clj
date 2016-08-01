(ns datumbazo.driver.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]))

(defrecord Driver [connection])

(defmethod d/find-driver 'clojure.java.jdbc
  [db & [opts]]
  (map->Driver db))

(defn- connected?
  "Return true if `driver` is connected, otherwise false."
  [driver]
  (:connection driver))

(defn- begin
  "Begin a new database transaction."
  [driver & [{:keys [isolation read-only?]}]]
  {:pre [(connected? driver)]}
  ;; Taken from clojure.java.jdbc/db-transaction*
  (if (zero? (jdbc/get-level driver))
    (let [con (jdbc/db-find-connection driver)]
      (let [nested-db (#'jdbc/inc-level driver)]
        (io!
         (when isolation
           (.setTransactionIsolation con (isolation #'jdbc/isolation-levels)))
         (when read-only?
           (.setReadOnly con true))
         (.setAutoCommit con false)
         nested-db)))
    (do
      (when (and isolation
                 (let [con (jdbc/db-find-connection driver)]
                   (not= (isolation #'jdbc/isolation-levels)
                         (.getTransactionIsolation con))))
        (let [msg "Nested transactions may not have different isolation levels"]
          (throw (IllegalStateException. msg))))
      (#'jdbc/inc-level driver))))

(defn- connect
  "Connect to the database."
  [driver & [opts]]
  (->> (jdbc/get-connection
        (if (:datasource driver)
          (select-keys driver [:datasource])
          (util/format-url driver)))
       (assoc driver :connection)))

(defn- connection
  "Return the current database connection."
  [driver]
  (:connection driver))

(defn commit
  "Commit the currenbt database transaction."
  [driver & [opts]]
  {:pre [(connected? driver)]}
  (if (jdbc/db-is-rollback-only driver)
    (.rollback (:connection driver))
    (.commit (:connection driver)))
  driver)

(defn- disconnect
  "Disconnect from the database."
  [driver]
  {:pre [(connected? driver)]}
  (when-let [connection (:connection driver)]
    (.close connection))
  (assoc driver :connection nil))

(defn- fetch
  "Query the database and fetch the result."
  [driver sql & [opts]]
  {:pre [(connected? driver)]}
  (let [identifiers (or (:sql-keyword driver) str/lower-case)
        opts (merge {:identifiers identifiers} opts)]
    (try
      (jdbc/query driver sql opts)
      (catch Exception e
        (util/throw-sql-ex-info e sql)))))

(defn- execute
  "Execute a SQL statement against the database."
  [driver sql & [opts]]
  {:pre [(connected? driver)]}
  (try
    (d/row-count (jdbc/execute! driver sql))
    (catch Exception e
      (util/throw-sql-ex-info e sql))))

(defn- prepare-statement
  "Return a prepared statement for the `sql` statement."
  [driver sql & [opts]]
  {:pre [(connected? driver)]}
  (let [prepared (jdbc/prepare-statement (connection driver) (first sql) opts)]
    (dorun (map-indexed (fn [i v] (jdbc/set-parameter v prepared (inc i))) (rest sql)))
    prepared))

(defn- rollback!
  "Mark the current database transaction for rollback."
  [driver & [opts]]
  (jdbc/db-set-rollback-only! driver)
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

(extend-protocol jdbc/IResultSetReadColumn

  org.postgresql.jdbc.PgArray
  (result-set-read-column [val rsmeta idx]
    (io/decode-array val))

  org.postgresql.util.PGobject
  (result-set-read-column [val rsmeta idx]
    (io/decode-pgobject val))

  org.postgis.PGgeometry
  (result-set-read-column [val rsmeta idx]
    (io/decode-pggeometry val))

  java.sql.Date
  (result-set-read-column [val rsmeta idx]
    (io/decode-date val))

  java.sql.Timestamp
  (result-set-read-column [val rsmeta idx]
    (io/decode-date val)))

(extend-protocol jdbc/ISQLValue

  clojure.lang.BigInt
  (sql-value [big-int]
    (long big-int))

  java.util.Date
  (sql-value [date]
    (io/encode-timestamp date))

  org.joda.time.DateTime
  (sql-value [date-time]
    (io/encode-timestamp date-time))

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
    (io/encode-pggeometry geometry)))
