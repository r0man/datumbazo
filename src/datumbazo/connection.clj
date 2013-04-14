(ns datumbazo.connection
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [rename-keys]]
            [clojure.string :refer [split]]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]
            [inflections.util :refer [parse-integer]]
            [datumbazo.util :as util]))

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
  (let [url (util/parse-db-url db-url)]
    (-> (assoc url :classname "com.mysql.jdbc.Driver")
        (update-in [:spec] #(rename-keys %1 {:username :user})))))

(defmethod connection-spec :oracle [db-url]
  (let [url (util/parse-db-url db-url)]
    (assoc url
      :classname "oracle.jdbc.driver.OracleDriver"
      :spec {:subprotocol "oracle:thin"
             :subname (str ":" (:username url) "/" (:password url) "@" (util/format-server url)
                           ":" (:db url))})))

(defmethod connection-spec :postgresql [db-url]
  (let [url (util/parse-db-url db-url)]
    (-> (assoc url :classname "org.postgresql.Driver")
        (assoc-in [:spec :user] (:username url)))))

(defmethod connection-spec :sqlite [db-url]
  (if-let [matches (re-matches #"(([^:]+):)?([^:]+):([^?]+)(\?(.*))?" (str db-url))]
    {:classname "org.sqlite.JDBC"
     :params (util/parse-params (nth matches 5))
     :pool (keyword (or (nth matches 2) :jdbc))
     :spec {:subname (nth matches 4)
            :subprotocol (nth matches 3)}}))

(defmethod connection-spec :sqlserver [db-url]
  (let [url (util/parse-db-url db-url)]
    (assoc url
      :classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      :spec {:subprotocol "sqlserver"
             :subname (str "//" (util/format-server url) ";"
                           "database=" (:db url) ";"
                           "user=" (:username url) ";"
                           "password=" (:password url))})))

(defmulti connection-pool
  "Returns the connection pool for `db-spec`."
  (fn [db-spec] (:pool db-spec)))

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
  (if-let [db-spec (connection-spec db-url)]
    (connection-pool db-spec)
    (util/illegal-argument-exception "Can't connect to: %s" db-url)))

(util/defn-memo cached-connection [db-url]
  "Returns the cached database connection for `db-url`."
  (connection db-url))

(defmacro with-connection
  "Evaluates `body` with a connection to `db-name`."
  [db-name & body]
  `(binding [*connection* (cached-connection (connection-url ~db-name))]
     (jdbc/with-naming-strategy *naming-strategy*
       (jdbc/with-connection (:spec *connection*)
         ~@body))))

(defn wrap-connection
  "Wraps a connection to `db-name` around the Ring `handler`"
  [handler db-name]
  (fn [request]
    (with-connection db-name
      (handler request))))


(connection-pool (connection-spec (connection-url :test-db)))