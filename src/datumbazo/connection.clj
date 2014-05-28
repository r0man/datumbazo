(ns datumbazo.connection
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :refer [infof warnf]]
            [com.stuartsierra.component :as component]
            [clojure.string :refer [join]]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]
            [no.en.core :refer [parse-integer]]
            [datumbazo.util :as util]
            [sqlingvo.db :as db]))

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
                     ":" (:database spec))))))

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
                     "database=" (:database spec) ";"
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
       "/" (:database db-spec)
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


(defn connect
  "Establish the database connection for `component`."
  [component]
  (if (:connection component)
    (throw (ex-info "Database connection already established." component)))
  (let [connection (jdbc/get-connection component)]
    (infof "Database connection to %s on %s established."
           (:database component) (:host component))
    (assoc component :connection connection) ))

(defn disconnect
  "Close the database connection for `component`."
  [component]
  (if-let [connection (:connection component)]
    (do (.close connection)
        (infof "Database connection to %s on %s closed."
               (:database component) (:host component)))
    (warnf "Database connection already closed."))
  (dissoc component :connection))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [component]
    (connect component))
  (stop [component]
    (disconnect component)))

;; (defmacro deflifecycle [& dbs]
;;   `(do ~@(for [db# dbs]
;;            `(extend-type ~db#
;;               Lifecycle
;;               (start [component#]
;;                 (connect component#))
;;               (stop [component#]
;;                 (disconnect component#))))))

;; (deflifecycle
;;   sqlingvo.db.mysql
;;   sqlingvo.db.postgresql
;;   sqlingvo.db.oracle
;;   sqlingvo.db.sqlite
;;   sqlingvo.db.sqlserver
;;   sqlingvo.db.vertica)
