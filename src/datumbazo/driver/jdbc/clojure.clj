(ns datumbazo.driver.jdbc.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]))

(defrecord Driver [connection]
  d/IConnection
  (-connect [driver opts]
    (->> (jdbc/get-connection
          (if (:datasource driver)
            (select-keys driver [:datasource])
            (util/format-url driver)))
         (assoc driver :connection)))
  (-connection [driver]
    connection)
  (-disconnect [driver]
    (some-> connection .close)
    (assoc driver :connection nil))

  d/IExecute
  (-execute [driver sql opts]
    (d/row-count (jdbc/execute! driver sql)))

  d/IFetch
  (-fetch [driver sql opts]
    (let [identifiers (or (:sql-keyword driver) str/lower-case)
          opts (merge {:identifiers identifiers} opts)]
      (jdbc/query driver sql opts)))

  d/IPrepareStatement
  (-prepare-statement [driver sql opts]
    (let [prepared (jdbc/prepare-statement connection (first sql) opts)]
      (dorun (map-indexed (fn [i v] (jdbc/set-parameter v prepared (inc i))) (rest sql)))
      prepared))

  d/ITransaction
  (-begin [driver [{:keys [isolation read-only?]}]]
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
  (-commit [driver opts]
    (if (jdbc/db-is-rollback-only driver)
      (.rollback connection)
      (.commit connection))
    driver)
  (-rollback [driver opts]
    (jdbc/db-set-rollback-only! driver)
    (.rollback connection)
    driver))

(defmethod d/find-driver 'clojure.java.jdbc
  [db & [opts]]
  (map->Driver db))

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
