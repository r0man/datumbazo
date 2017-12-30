(ns datumbazo.driver.jdbc.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]))

(defrecord Driver [connection]
  d/IConnection
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

  d/IExecute
  (-execute [this db sql opts]
    (d/row-count (jdbc/execute! this sql)))

  d/IFetch
  (-fetch [this db sql opts]
    (let [identifiers (or (:sql-keyword db) str/lower-case)
          opts (merge {:identifiers identifiers} opts)]
      (jdbc/query this sql opts)))

  d/IPrepareStatement
  (-prepare-statement [this db sql opts]
    (let [prepared (jdbc/prepare-statement connection (first sql) opts)]
      (dorun (map-indexed (fn [i v] (jdbc/set-parameter v prepared (inc i))) (rest sql)))
      prepared))

  d/ITransaction
  (-begin [this db [{:keys [isolation read-only?]}]]
    ;; Taken from clojure.java.jdbc/db-transaction*
    (if (zero? (jdbc/get-level this))
      (let [con (jdbc/db-find-connection this)]
        (let [nested-db (#'jdbc/inc-level this)]
          (io!
           (when isolation
             (.setTransactionIsolation con (isolation #'jdbc/isolation-levels)))
           (when read-only?
             (.setReadOnly con true))
           (.setAutoCommit con false)
           nested-db)))
      (do
        (when (and isolation
                   (let [con (jdbc/db-find-connection this)]
                     (not= (isolation #'jdbc/isolation-levels)
                           (.getTransactionIsolation con))))
          (let [msg "Nested transactions may not have different isolation levels"]
            (throw (IllegalStateException. msg))))
        (#'jdbc/inc-level this))))

  (-commit [this db opts]
    (if (jdbc/db-is-rollback-only this)
      (.rollback connection)
      (.commit connection))
    this)

  (-rollback [this db opts]
    (jdbc/db-set-rollback-only! this)
    (.rollback connection)
    this))

(defmethod d/find-driver 'clojure.java.jdbc
  [db & [opts]]
  (map->Driver {}))

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
