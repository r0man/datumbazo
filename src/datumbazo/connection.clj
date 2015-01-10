(ns datumbazo.connection
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [replace]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [datumbazo.util :as util]
            [no.en.core :refer [parse-integer]]
            [sqlingvo.core :refer [ast sql sql-keyword]])
  (:import (java.sql Connection SQLException)))

(defn start-transaction
  "Start a database transaction."
  [db]
  (let [connection (jdbc/get-connection db)]
    (.setAutoCommit connection false)
    (assoc db :rollback (atom true) :level 1)))

(defn rollback-transaction
  "Rollback a database transaction."
  [db]
  (when (and (:rollback db) @(:rollback db))
    (.rollback (jdbc/get-connection db))))

(defmulti connect
  "Connect to `db`."
  (fn [db] (:pool db)))

(defn- connect-datasource [db datasource]
  (log/infof "Database connection pool (%s) to %s on %s established."
             (name (:pool db)) (:name db) (:server-name db))
  (assoc db :datasource datasource))

(defmethod connect :bonecp [db]
  (connect-datasource
   db (util/invoke-constructor
       "com.jolbox.bonecp.BoneCPDataSource"
       (doto (util/invoke-constructor "com.jolbox.bonecp.BoneCPConfig")
         (.setJdbcUrl (str "jdbc:" (name (:subprotocol db)) ":" (:subname db)))
         (.setUsername (:user db))
         (.setPassword (:password db))
         (.setDefaultAutoCommit (not (true? (:test db))))))))

(defmethod connect :c3p0 [{:keys [params] :as db}]
  (connect-datasource
   db (doto (util/invoke-constructor "com.mchange.v2.c3p0.ComboPooledDataSource")
        (.setJdbcUrl (str "jdbc:" (name (:subprotocol db)) ":" (:subname db)))
        (.setUser (:user db))
        (.setPassword (:password db))
        (.setAcquireRetryAttempts (parse-integer (or (:acquire-retry-attempts params) 30)))
        (.setInitialPoolSize (parse-integer (or (:initial-pool-size params) 3)))
        (.setMaxIdleTime (parse-integer (or (:max-idle-time params) (* 3 60 60))))
        (.setMaxIdleTimeExcessConnections (parse-integer (or (:max-idle-time-excess-connections params) (* 30 60))))
        (.setMaxPoolSize (parse-integer (or (:max-pool-size params) 15)))
        (.setMinPoolSize (parse-integer (or (:min-pool-size params) 3))))))

(defmethod connect :default [component]
  (if (:connection component)
    (throw (ex-info "Database connection already established." component)))
  (let [connection (jdbc/get-connection (dissoc component :name))
        component (jdbc/add-connection component connection)]
    (log/infof "Database connection to %s on %s established."
               (:name component) (:server-name component))
    (if (:test component)
      (start-transaction component)
      component)))

(defmulti disconnect
  "Disconnect from `db`."
  (fn [db] (:pool db)))

(defn- disconnect-datasource [db]
  (if-let [datasource (:datasource db)]
    (do ;; (if (:test db) (rollback-transaction db))
      (.close datasource)
      (log/infof "Database connection pool (%s) to %s on %s closed."
                 (name (:pool db)) (:name db) (:server-name db)))
    (log/warnf "Database connection already closed."))
  (assoc db :datasource nil :savepoint nil))

(defmethod disconnect :default [db]
  (if-let [connection (:connection db)]
    (do (if (:test db) (rollback-transaction db))
        (.close connection)
        (log/infof "Database connection to %s on %s closed."
                   (:name db) (:server-name db)))
    (log/warnf "Database connection already closed."))
  (assoc db :connection nil :savepoint nil))

(defmethod disconnect :bonecp [db]
  (disconnect-datasource db))

(defmethod disconnect :c3p0 [db]
  (disconnect-datasource db))

(defmacro with-connection [[connection-sym db] & body]
  `(cond
     (instance? Connection (:connection ~db))
     (let [~connection-sym (:connection ~db)]
       ~@body)
     (:datasource ~db)
     (with-open [~connection-sym (jdbc/get-connection ~db)]
       ~@body)))

(defn- db
  "Return the db from `ast`."
  [ast]
  (assert db (str "No db:" (:db ast)))
  (:db ast))

(defn- prepare-stmt
  "Compile `stmt` and return a java.sql.PreparedStatement from `db`."
  [stmt]
  (with-connection [connection (db (ast stmt))]
    (let [[sql & args] (sql stmt)
          stmt (jdbc/prepare-statement connection sql)]
      (dorun (map-indexed (fn [i v] (jdbc/set-parameter v stmt (inc i))) args))
      stmt)))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [sql (first (sql stmt))]
    (with-open [stmt (prepare-stmt stmt)]
      (if (.startsWith (str stmt) (replace sql #"\?.*" ""))
        (str stmt)
        (throw (UnsupportedOperationException. "Sorry, sql-str not supported by SQL driver."))))))

(defn- run-copy
  [ast & [opts]]
  ;; TODO: Get rid of sql-str
  (with-connection [connection (db ast)]
    (let [compiled (sql-str ast)]
      (with-open [stmt (.prepareStatement connection compiled)]
        (.execute stmt)))))

(defn- run-query
  [ast & [opts]]
  (let [compiled (sql ast)
        opts (merge {:identifiers #(sql-keyword (db ast) %1)} opts)
        query #(apply jdbc/query %1 compiled (apply concat opts))]
    (if (:transaction? opts)
      (jdbc/with-db-transaction [t-db (db ast)]
        (query t-db))
      (query (db ast)))))

(defn- run-prepared
  [ast & [{:keys [transaction?] :as opts}]]
  (let [compiled (sql ast)]
    (->> (jdbc/execute! (db ast) compiled :transaction? transaction?)
         (map #(hash-map :count %1)))))

(defn run*
  "Compile and run `stmt` against the database and return the rows."
  [stmt & [opts]]
  (let [{:keys [op returning] :as ast} (ast stmt)]
    (try (cond
           (= :copy op)
           (run-copy ast opts)
           (= :select op)
           (run-query ast opts)
           (and (= :with op)
                (or (= :select (:op (:query ast)))
                    (:returning (:query ast))))
           (run-query ast opts)
           returning
           (run-query ast opts)
           :else (run-prepared ast opts))
         (catch Exception e
           (if (or (instance? SQLException e)
                   (instance? SQLException (.getCause e)))
             (throw (ex-info (format "Can't execute SQL statement: %s\n%s"
                                     (pr-str (sql stmt))
                                     (.getMessage e))
                             ast e))
             (throw e))))))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [db]
    (connect db))
  (stop [db]
    (disconnect db)))
