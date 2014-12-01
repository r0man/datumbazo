(ns datumbazo.connection
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [datumbazo.util :as util]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]
            [no.en.core :refer [parse-integer]]
            [sqlingvo.core :refer [ast sql sql-keyword]]
            [sqlingvo.db :as db])
  (:import (java.sql SQLException))
  (:refer-clojure :exclude [replace]))

(def ^:dynamic *connection* nil)

(def ^:dynamic *naming-strategy*
  {:entity underscore :keyword dasherize})

(defn connection-url
  "Lookup the JDBC connection url for `db-name` via environ."
  [db-name]
  (cond
   ;; TODO: Check url.
   (string? db-name)
   db-name
   (keyword? db-name)
   (or (env db-name)
       (util/illegal-argument-exception "Can't find connection url: %s" db-name))))

(defmulti connection-spec
  "Parse `db-url` and return the connection spec."
  (fn [db-url] (keyword (util/parse-subprotocol db-url))))

(defmethod connection-spec :mysql [db-url]
  (let [spec (util/parse-db-url db-url)]
    (db/mysql
     (assoc spec
       :classname "com.mysql.jdbc.Driver"))))

(defmethod connection-spec :oracle [db-url]
  (let [spec (util/parse-db-url db-url)]
    (db/oracle
     (assoc spec
       :classname "oracle.jdbc.driver.OracleDriver"
       :subprotocol "oracle:thin"
       :subname (str ":" (:username spec) "/" (:password spec) "@" (util/format-server spec)
                     ":" (:name spec))))))

(defmethod connection-spec :postgresql [db-url]
  (let [spec (util/parse-db-url db-url)]
    (db/postgresql
     (assoc spec
       :classname "org.postgresql.Driver"))))

(defmethod connection-spec :vertica [db-url]
  (let [spec (util/parse-db-url db-url)]
    (db/vertica
     (assoc spec
       :classname "com.vertica.jdbc.Driver"))))

(defmethod connection-spec :sqlite [db-url]
  (if-let [matches (re-matches #"(([^:]+):)?([^:]+):([^?]+)(\?(.*))?" (str db-url))]
    (db/sqlite
     {:classname "org.sqlite.JDBC"
      :params (util/parse-params (nth matches 5))
      :db-pool (keyword (or (nth matches 2) :jdbc))
      :subname (nth matches 4)
      :subprotocol (nth matches 3)})))

(defmethod connection-spec :sqlserver [db-url]
  (let [spec (util/parse-db-url db-url)]
    (db/sqlserver
     (assoc spec
       :classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
       :subprotocol "sqlserver"
       :subname (str "//" (util/format-server spec) ";"
                     "database=" (:name spec) ";"
                     "user=" (:username spec) ";"
                     "password=" (:password spec))))))

(defmulti connection-pool
  "Returns the connection pool for `db-spec`."
  (fn [db-spec] (:db-pool db-spec)))

(defmethod connection-pool :bonecp [db-spec]
  (let [config (util/invoke-constructor "com.jolbox.bonecp.BoneCPConfig")]
    (.setJdbcUrl config (str "jdbc:" (name (:subprotocol (:spec db-spec))) ":" (:subname (:spec db-spec))))
    (.setUsername config (:username db-spec))
    (.setPassword config (:password db-spec))
    (assoc db-spec :spec {:datasource (util/invoke-constructor "com.jolbox.bonecp.BoneCPDataSource" config)})))

(defmethod connection-pool :c3p0 [db-spec]
  (let [params (:params db-spec)
        datasource (util/invoke-constructor "com.mchange.v2.c3p0.ComboPooledDataSource")]
    (.setJdbcUrl datasource (str "jdbc:" (name (:subprotocol (:spec db-spec))) ":" (:subname (:spec db-spec))))
    (.setUser datasource (:username db-spec))
    (.setPassword datasource (:password db-spec))
    (.setAcquireRetryAttempts datasource (parse-integer (or (:acquire-retry-attempts params) 1))) ; TODO: Set back to 30
    (.setInitialPoolSize datasource (parse-integer (or (:initial-pool-size params) 3)))
    (.setMaxIdleTime datasource (parse-integer (or (:max-idle-time params) (* 3 60 60))))
    (.setMaxIdleTimeExcessConnections datasource (parse-integer (or (:max-idle-time-excess-connections params) (* 30 60))))
    (.setMaxPoolSize datasource (parse-integer (or (:max-pool-size params) 15)))
    (.setMinPoolSize datasource (parse-integer (or (:min-pool-size params) 3)))
    (assoc db-spec :spec {:datasource datasource})))

(defmethod connection-pool :jdbc [db-spec]
  db-spec)

(defn connection [db-url]
  "Returns the database connection for `db-name`."
  (if (map? db-url)
    db-url
    (if-let [db-spec (connection-spec db-url)]
      (connection-pool db-spec)
      (util/illegal-argument-exception "Can't connect to: %s" db-url))))

(util/defn-memo cached-connection [db-url]
  "Returns the cached database connection for `db-url`."
  (connection db-url))

(defn jdbc-url
  "Returns a JDBC url from the `db-spec`."
  [db-spec]
  (str "jdbc:" (:subprotocol db-spec) "://"
       (:host db-spec)
       (if-let [port (:port db-spec)]
         (str ":" port))
       "/" (:name db-spec)
       (str "?" (join "&" (map (fn [[k v]] (str (name k) "=" v))
                               (seq (assoc (:params db-spec)
                                      :user (:user db-spec)
                                      :password (:password db-spec))))))))

(defmacro with-connection
  [[symbol db] & body]
  `(let [db# (connection ~db)]
     (if (and (map? db#) (:connection db#))
       (let [~symbol db#]
         ~@body)
       (with-open [connection# (jdbc/get-connection db#)]
         (let [~symbol (assoc db#
                         :connection connection#
                         :connection-string ~db
                         :level 0)]
           ~@body)))))

(defn start-transaction [db]
  (.setAutoCommit (:connection db) false)
  (assoc db :rollback (atom true) :level 1))

(defn connect
  "Establish the database connection for `component`."
  [component]
  (if (:connection component)
    (throw (ex-info "Database connection already established." component)))
  (let [connection (jdbc/get-connection component)
        component (jdbc/add-connection component connection)]
    (log/infof "Database connection to %s on %s established."
               (:name component) (:server-name component))
    (if (:test component)
      (start-transaction component)
      component)))

(defn disconnect
  "Close the database connection for `component`."
  [component]
  (if-let [connection (:connection component)]
    (do (when (and (:rollback component) @(:rollback component))
          (.rollback connection))
        (.close connection)
        (log/infof "Database connection to %s on %s closed."
                   (:name component) (:server-name component)))
    (log/warnf "Database connection already closed."))
  (dissoc component :connection :savepoint))

(defn- db
  "Return the db from `ast`."
  [ast]
  (assert db (str "No db:" (:db ast)))
  (:db ast))

(defn- prepare-stmt
  "Compile `stmt` and return a java.sql.PreparedStatement from `db`."
  [stmt]
  (let [[sql & args] (sql stmt)
        stmt (jdbc/prepare-statement (:connection (db (ast stmt))) sql)]
    (doall (map-indexed (fn [i v] (.setObject stmt (inc i) v)) args))
    stmt))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [sql (first (sql stmt))
        stmt (prepare-stmt stmt)]
    (if (.startsWith (str stmt) (replace sql #"\?.*" ""))
      (str stmt)
      (throw (UnsupportedOperationException. "Sorry, sql-str not supported by SQL driver.")))))

(defn- run-copy
  [ast & {:keys [transaction?]}]
  ;; TODO: Get rid of sql-str
  (let [compiled (sql-str ast)
        stmt (.prepareStatement (:connection (db ast)) compiled)]
    (.execute stmt)))

(defn- run-query
  [ast & {:keys [transaction?]}]
  (let [compiled (sql ast)
        identifiers #(sql-keyword (db ast) %1)
        query #(jdbc/query %1 compiled :identifiers identifiers)]
    (if transaction?
      (jdbc/with-db-transaction [t-db (db ast)]
        (query t-db))
      (query (db ast)))))

(defn- run-prepared
  [ast & {:keys [transaction?]}]
  (let [compiled (sql ast)]
    (->> (jdbc/db-do-prepared (db ast) transaction? (first compiled) (rest compiled))
         (map #(hash-map :count %1)))))

(defn run*
  "Compile and run `stmt` against the database and return the rows."
  [stmt & [{:keys [transaction?]}]]
  (let [{:keys [op returning] :as ast} (ast stmt)]
    (try (cond
          (= :copy op)
          (run-copy ast :transaction? transaction?)
          (= :select op)
          (run-query ast :transaction? transaction?)
          (and (= :with op)
               (or (= :select (:op (:query ast)))
                   (:returning (:query ast))))
          (run-query ast :transaction? transaction?)
          returning
          (run-query ast :transaction? transaction?)
          :else (run-prepared ast :transaction? transaction?))
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

(defmacro with-db
  "Evaluate `body` within the context of a database connection."
  [[db-sym component] & body]
  `(let [component# (component/start ~component)
         ~db-sym component#]
     (try ~@body
          (finally (component/stop component#)))))
