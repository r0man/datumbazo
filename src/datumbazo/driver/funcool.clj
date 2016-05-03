(ns datumbazo.driver.funcool
  (:require [clojure.string :as str]
            [datumbazo.driver.core :refer :all]
            [datumbazo.io :as io]
            [datumbazo.util :as util]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [jdbc.impl :as impl]
            [datumbazo.db :refer [format-url]]))

(defn- tx-strategy [db & [opts]]
  (or (:strategy opts)
      (-> db :connection meta :tx-strategy)
      jdbc/*default-tx-strategy*))

(defmethod begin 'jdbc.core [db & [opts]]
  {:pre [(connected? db)]}
  (let [connection (:connection db)
        tx-strategy (tx-strategy db)]
    (assoc db :connection (proto/begin! tx-strategy connection opts))))

(defmethod apply-transaction 'jdbc.core [db f & [opts]]
  {:pre [(connected? db)]}
  (jdbc/atomic-apply
   (:connection db)
   (fn [connection]
     (f (assoc db :connection connection)))
   opts))

(defmethod close-connection 'jdbc.core [db]
  {:pre [(connected? db)]}
  (when (some-> db :connection meta :rollback deref)
    (.rollback (connection db)))
  (when-let [connection (:connection db)]
    (.close connection))
  (assoc db :connection nil))

(defmethod commit 'jdbc.core [db & [opts]]
  {:pre [(connected? db)]}
  (let [connection (:connection db)
        tx-strategy (tx-strategy db)]
    (assoc db :connection (proto/commit! tx-strategy connection opts))))

(defmethod connection 'jdbc.core [db & [opts]]
  (some-> db :connection proto/connection))

(defmethod fetch 'jdbc.core [db sql & [opts]]
  {:pre [(connected? db)]}
  (let [identifiers (or (:sql-keyword db) str/lower-case)
        opts (merge {:identifiers identifiers} opts)]
    (try
      (jdbc/fetch (:connection db) sql opts)
      (catch Exception e
        (util/throw-sql-ex-info e sql)))))

(defmethod execute 'jdbc.core [db sql & [opts]]
  {:pre [(connected? db)]}
  (try (row-count (jdbc/execute (:connection db) sql))
       (catch Exception e
         (util/throw-sql-ex-info e sql))))

(defmethod open-connection 'jdbc.core [db & [opts]]
  (let [db (->> (if-let [datasource (:datasource db)]
                  (jdbc/connection datasource)
                  (jdbc/connection (format-url db)))
                (assoc db :connection))]
    (if (or (:rollback? db)
            (:rollback? opts))
      (-> (begin db) (rollback!))
      db)))

(defmethod prepare-statement 'jdbc.core [db sql & [opts]]
  {:pre [(connected? db)]}
  (proto/prepared-statement sql (connection db) opts))

(defmethod rollback! 'jdbc.core [db]
  {:pre [(connected? db)]}
  (jdbc/set-rollback! (:connection db))
  db)

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
