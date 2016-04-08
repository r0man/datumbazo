(ns datumbazo.connection
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [replace]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as driver]
            [datumbazo.util :as util]
            [no.en.core :refer [parse-integer]]
            [sqlingvo.core :refer [ast sql]]))

(defmulti connect
  "Connect to `db`."
  (fn [db] (keyword (:pool db))))

(defn- connect-datasource [db datasource]
  (log/infof "Database connection pool (%s) to %s on %s established."
             (name (:pool db)) (:name db) (:server-name db))
  (driver/open-db (assoc db :datasource datasource)))

(defmethod connect :bonecp [db]
  (connect-datasource
   db (util/invoke-constructor
       "com.jolbox.bonecp.BoneCPDataSource"
       (doto (util/invoke-constructor "com.jolbox.bonecp.BoneCPConfig")
         (.setJdbcUrl (str "jdbc:" (name (:subprotocol db)) ":" (:subname db)))
         (.setUsername (:user db))
         (.setPassword (:password db))))))

(defmethod connect :c3p0 [{:keys [query-params] :as db}]
  (connect-datasource
   db (doto (util/invoke-constructor "com.mchange.v2.c3p0.ComboPooledDataSource")
        (.setJdbcUrl (str "jdbc:" (name (:subprotocol db)) ":" (:subname db)))
        (.setUser (:user db))
        (.setPassword (:password db))
        (.setAcquireRetryAttempts (parse-integer (or (:acquire-retry-attempts query-params) 30)))
        (.setInitialPoolSize (parse-integer (or (:initial-pool-size query-params) 3)))
        (.setMaxIdleTime (parse-integer (or (:max-idle-time query-params) (* 3 60 60))))
        (.setMaxIdleTimeExcessConnections (parse-integer (or (:max-idle-time-excess-connections query-params) (* 30 60))))
        (.setMaxPoolSize (parse-integer (or (:max-pool-size query-params) 15)))
        (.setMinPoolSize (parse-integer (or (:min-pool-size query-params) 3))))))

(defmethod connect :default [db]
  (if (:connection db)
    (throw (ex-info "Database connection already established." {:db db})))
  (let [db (driver/open-db db)]
    (log/infof "Database connection to %s on %s established."
               (:name db) (:server-name db))
    db))

(defmulti disconnect
  "Disconnect from `db`."
  (fn [db] (:pool db)))

(defn- disconnect-datasource [db]
  (if-let [datasource (:datasource db)]
    (do (.close datasource)
        (log/infof "Database connection pool (%s) to %s on %s closed."
                   (name (:pool db)) (:name db) (:server-name db)))
    (log/warnf "Database connection already closed."))
  (assoc db :datasource nil :savepoint nil))

(defmethod disconnect :default [db]
  (if-let [connection (:connection db)]
    (do (.close connection)
        (log/infof "Database connection to %s on %s closed."
                   (:name db) (:server-name db)))
    (log/warnf "Database connection already closed."))
  (assoc db :connection nil :savepoint nil))

(defmethod disconnect :bonecp [db]
  (disconnect-datasource db))

(defmethod disconnect :c3p0 [db]
  (disconnect-datasource db))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [ast (ast stmt)]
    (with-open [stmt (driver/prepare-statement (:db ast) (sql ast))]
      (if (.startsWith (str stmt) (replace (first (sql ast)) #"\?.*" ""))
        (str stmt)
        (throw (UnsupportedOperationException. "Sorry, sql-str not supported by SQL driver."))))))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [db]
    (let [db (connect db)]
      (if (:rollback? db)
        (driver/begin db)
        db)))
  (stop [db]
    (when (:rollback? db)
      (driver/rollback! db))
    (disconnect db)))
