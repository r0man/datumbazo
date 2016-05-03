(ns datumbazo.driver.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.db :refer [format-url]]
            [datumbazo.driver.core :refer :all]
            [datumbazo.io :as io]
            [datumbazo.util :as util]))

(defmethod begin 'clojure.java.jdbc
  [db & [{:keys [isolation read-only?]}]]
  {:pre [(connected? db)]}
  ;; Taken from clojure.java.jdbc/db-transaction*
  (if (zero? (jdbc/get-level db))
    (let [con (jdbc/db-find-connection db)]
      (let [nested-db (#'jdbc/inc-level db)]
        (io!
         (when isolation
           (.setTransactionIsolation con (isolation #'jdbc/isolation-levels)))
         (when read-only?
           (.setReadOnly con true))
         (.setAutoCommit con false)
         nested-db)))
    (do
      (when (and isolation
                 (let [con (jdbc/db-find-connection db)]
                   (not= (isolation #'jdbc/isolation-levels)
                         (.getTransactionIsolation con))))
        (let [msg "Nested transactions may not have different isolation levels"]
          (throw (IllegalStateException. msg))))
      (#'jdbc/inc-level db))))

(defmethod apply-transaction 'clojure.java.jdbc [db f & [opts]]
  {:pre [(connected? db)]}
  (jdbc/db-transaction* db f opts))

(defmethod commit 'clojure.java.jdbc [db & [opts]]
  {:pre [(connected? db)]}
  (if (jdbc/db-is-rollback-only db)
    (.rollback (connection db))
    (.commit (connection db)))
  db)

(defmethod close-connection 'clojure.java.jdbc [db]
  {:pre [(connected? db)]}
  (when (and (:rollback db) (jdbc/db-is-rollback-only db))
    (.rollback (connection db)))
  (when-let [connection (:connection db)]
    (.close connection))
  (assoc db :connection nil))

(defmethod connection 'clojure.java.jdbc [db & [opts]]
  (:connection db))

(defmethod fetch 'clojure.java.jdbc [db sql & [opts]]
  {:pre [(connected? db)]}
  (let [identifiers (or (:sql-keyword db) str/lower-case)
        opts (merge {:identifiers identifiers} opts)]
    (try
      (jdbc/query db sql opts)
      (catch Exception e
        (util/throw-sql-ex-info e sql)))))

(defmethod execute 'clojure.java.jdbc [db sql & [opts]]
  {:pre [(connected? db)]}
  (try
    (row-count (jdbc/execute! db sql))
    (catch Exception e
      (util/throw-sql-ex-info e sql))))

(defmethod open-connection 'clojure.java.jdbc [db & [opts]]
  (let [connection (jdbc/get-connection
                    (if (:datasource db)
                      (select-keys db [:datasource])
                      (format-url db)))
        db (assoc db :connection connection)]
    (if (or (:rollback? db)
            (:rollback? opts))
      (-> (begin db) (rollback!))
      db)))

(defmethod prepare-statement 'clojure.java.jdbc [db sql & [opts]]
  {:pre [(connected? db)]}
  (let [prepared (jdbc/prepare-statement (connection db) (first sql) opts)]
    (dorun (map-indexed (fn [i v] (jdbc/set-parameter v prepared (inc i))) (rest sql)))
    prepared))

(defmethod rollback! 'clojure.java.jdbc [db]
  (jdbc/db-set-rollback-only! db)
  db)

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
