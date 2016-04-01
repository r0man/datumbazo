(ns datumbazo.driver.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.driver.core :refer :all]
            [datumbazo.io :as io]
            [datumbazo.util :as util]))

(defn- assert-connection [db]
  (when-not (or (:connection db) (:datasource  db))
    (throw (ex-info "No database connection or datasource!" {:db db}))))

(defmethod apply-transaction 'clojure.java.jdbc [db f & [opts]]
  (apply jdbc/db-transaction* db f (apply concat opts)))

(defmethod close-db 'clojure.java.jdbc [db]
  (.close (:connection db)))

(defmethod connection 'clojure.java.jdbc [db & [opts]]
  (jdbc/get-connection db))

(defmethod fetch 'clojure.java.jdbc [db sql & [opts]]
  (assert-connection db)
  (let [identifiers (or (:sql-keyword db) str/lower-case)
        opts (merge {:identifiers identifiers} opts)]
    (try
      (apply jdbc/query db sql (apply concat opts))
      (catch Exception e
        (util/throw-sql-ex-info e sql)))))

(defmethod execute 'clojure.java.jdbc [db sql & [opts]]
  (assert-connection db)
  (try
    (row-count (jdbc/execute! db sql))
    (catch Exception e
      (util/throw-sql-ex-info e sql))))

(defmethod open-db 'clojure.java.jdbc [db]
  (assoc db :connection (jdbc/get-connection db)))

(defmethod prepare-statement 'clojure.java.jdbc [db sql & [opts]]
  (assert-connection db)
  (let [opts (apply concat opts)
        prepared (apply jdbc/prepare-statement (connection db) (first sql) opts)]
    (dorun (map-indexed (fn [i v] (jdbc/set-parameter v prepared (inc i))) (rest sql)))
    prepared))

(defmethod rollback! 'clojure.java.jdbc [db]
  (jdbc/db-set-rollback-only! db))

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
