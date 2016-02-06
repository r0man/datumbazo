(ns datumbazo.driver.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.driver.core :refer :all]
            [datumbazo.io :as io]
            [sqlingvo.compiler :refer [compile-stmt]]))

(defmethod apply-transaction 'clojure.java.jdbc [db f & [opts]]
  (jdbc/db-transaction* db f))

(defmethod close-db 'clojure.java.jdbc [db]
  (.close (:connection db)))

(defmethod eval-db* 'clojure.java.jdbc
  [{:keys [db] :as ast} & [opts]]
  (let [sql (compile-stmt ast)]
    (case (:op ast)
      :except
      (jdbc/query db sql)
      :delete
      (if (:returning ast)
        (jdbc/query db sql)
        (row-count (jdbc/execute! db sql)))
      :insert
      (if (:returning ast)
        (jdbc/query db sql)
        (row-count (jdbc/execute! db sql)))
      :intersect
      (jdbc/query db sql)
      :select
      (jdbc/query db sql)
      :union
      (jdbc/query db sql)
      :update
      (if (:returning ast)
        (jdbc/query db sql)
        (row-count (jdbc/execute! db sql)))
      (row-count (jdbc/execute! db sql)))))

(defmethod open-db 'clojure.java.jdbc [db]
  (assoc db :connection (jdbc/get-connection db)))

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
