(ns datumbazo.driver.next.jdbc
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datumbazo.driver.core :as d]
            [datumbazo.io :as io]
            [datumbazo.util :as util]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as jdbc.prepare]
            [next.jdbc.result-set :as jdbc.result-set])
  (:import [java.sql Connection PreparedStatement]))

(defn kebab [s]
  (str/replace s #"_" "-"))

(defn as-kebab-maps [rs opts]
  (jdbc.result-set/as-modified-maps rs (assoc opts :qualifier-fn kebab :label-fn kebab)))

(defn as-unqualified-kebab-maps [rs opts]
  (jdbc.result-set/as-unqualified-modified-maps rs (assoc opts :label-fn kebab)))

(defn- rename-update-count [row]
  (set/rename-keys row {:next.jdbc/update-count :count}))

(defrecord Driver [config ^Connection connection]
  d/Connectable
  (-connect [this db opts]
    (->> (or connection
             (jdbc/get-connection
              (or (:datasource db)
                  {:classname (:classname db)
                   :dbname (:name db)
                   :dbtype (name (:scheme db))
                   :host (:server-name db)
                   :password (:password db)
                   :port (:server-port db)
                   :user (:username db)})))
         (assoc this :connection)))

  (-connection [this db]
    connection)

  (-disconnect [this db]
    (some-> connection .close)
    (assoc this :connection nil))

  d/Executeable
  (-execute-all [this db sql opts]
    (->> (jdbc/execute! connection sql (merge config opts))
         (mapv rename-update-count)))
  (-execute-one [this db sql opts]
    (->> (jdbc/execute! connection sql (merge config opts))
         (mapv rename-update-count)))

  d/Preparable
  (-prepare [this db sql opts]
    (jdbc/prepare connection sql (merge config opts)))

  d/Transactable
  (-transact [this db f opts]
    (jdbc/transact (:connection this) #(f (assoc-in db [:driver :connection] %)) (merge config opts))))

(defmethod d/driver :next.jdbc
  [db & [opts]]
  (map->Driver {:config (:next.jdbc db)}))

(extend-protocol jdbc.result-set/ReadableColumn
  java.sql.Date
  (read-column-by-label [val _]
    (io/decode-date val))
  (read-column-by-index [val _ _]
    (io/decode-date val))

  java.sql.Timestamp
  (read-column-by-label [val _]
    (io/decode-date val))
  (read-column-by-index [val _ _]
    (io/decode-date val)))

(extend-protocol jdbc.prepare/SettableParameter
  clojure.lang.BigInt
  (set-parameter [^clojure.lang.BigInt v ^PreparedStatement ps ^long i]
    (.setObject ps i v))

  java.util.Date
  (set-parameter [^java.util.Date v ^PreparedStatement ps ^long i]
    (.setTimestamp ps i (java.sql.Timestamp. (.getTime v))))

  java.time.Instant
  (set-parameter [^java.time.Instant v ^PreparedStatement ps ^long i]
    (.setTimestamp ps i (java.sql.Timestamp/from v)))

  java.time.LocalDate
  (set-parameter [^java.time.LocalDate v ^PreparedStatement ps ^long i]
    (.setTimestamp ps i (java.sql.Timestamp/valueOf (.atStartOfDay v))))

  java.time.LocalDateTime
  (set-parameter [^java.LocalDateTime v ^PreparedStatement ps ^long i]
    (.setTimestamp ps i (java.sql.Timestamp/valueOf v))))

;; Joda Time

(util/with-library-loaded :joda-time

  (extend-protocol jdbc.prepare/SettableParameter
    org.joda.time.DateTime
    (set-parameter [^org.joda.time.DateTime v ^PreparedStatement ps ^long i]
      (.setTimestamp ps i (io/encode-timestamp v)))))

;; PostgreSQL

(util/with-library-loaded :postgresql

  (extend-protocol jdbc.result-set/ReadableColumn
    org.postgresql.jdbc.PgArray
    (read-column-by-label [val _]
      (io/decode-array val))
    (read-column-by-index [val _ _]
      (io/decode-array val))

    org.postgresql.util.PGobject
    (read-column-by-label [val _]
      (io/decode-pgobject val))
    (read-column-by-index [val _ _]
      (io/decode-pgobject val))))

;; PostGIS

(util/with-library-loaded :postgis

  (extend-protocol jdbc.result-set/ReadableColumn
    org.postgis.PGgeo
    (read-column-by-label [val _]
      (io/decode-pggeometry val))
    (read-column-by-index [val _ _]
      (io/decode-pggeometry val)))

  (extend-protocol jdbc.prepare/SettableParameter
    org.postgis.LineString
    (set-parameter [^org.postgis.LineString v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))

    org.postgis.LinearRing
    (set-parameter [^org.postgis.LinearRing v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))

    org.postgis.MultiLineString
    (set-parameter [^org.postgis.MultiLineString v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))

    org.postgis.MultiPoint
    (set-parameter [^org.postgis.MultiPoint v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))

    org.postgis.MultiPolygon
    (set-parameter [^org.postgis.MultiPolygon v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))

    org.postgis.Point
    (set-parameter [^org.postgis.Point v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))

    org.postgis.Polygon
    (set-parameter [^org.postgis.Polygon v ^PreparedStatement ps ^long i]
      (.setObject ps i (io/encode-pggeometry v)))))
