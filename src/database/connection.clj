(ns database.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank?]]
            [database.util :refer [defn-memo parse-url parse-integer]]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]))

(def ^:dynamic *naming-strategy*
  {:entity underscore :keyword dasherize})

(def ^:dynamic *pool-params*
  {:max-idle-time (* 3 60 60)
   :max-idle-time-excess-connections (* 30 60)
   :max-pool-size 15})

(defn connection-url
  "Lookup the database connection url for `db-name`."
  [db-name]
  (if-let [url (env db-name)]
    url (throw (IllegalArgumentException. (format "Can't find database url via environ: %s" db-name)))))

(defn connection-spec
  "Lookup the database connection url for `db-name` and return the parsed db-spec."
  [db-name] (parse-url (connection-url db-name)))

(defn make-connection-pool
  "Make a C3P0 connection pool."
  [db-spec]
  (cond
   (keyword? db-spec)
   (make-connection-pool (connection-url db-spec))
   (string? db-spec)
   (make-connection-pool (parse-url db-spec))
   (map? db-spec)
   (let [params (merge *pool-params* (:params db-spec))]
     {:datasource
      (doto (ComboPooledDataSource.)
        (.setDriverClass (:classname db-spec))
        (.setJdbcUrl (str "jdbc:" (:subprotocol db-spec) ":" (:subname db-spec)))
        (.setUser (:user db-spec))
        (.setPassword (:password db-spec))
        (.setMaxIdleTimeExcessConnections (parse-integer (:max-idle-time-excess-connections params)))
        (.setMaxIdleTime (parse-integer (:max-idle-time params)))
        (.setMaxPoolSize (parse-integer (:max-pool-size params))))})))

(defn resolve-connection
  "Resolve the `db-spec` and return a connection map."
  [db-spec]
  (cond
   (nil? db-spec)
   (throw (IllegalArgumentException. (format "Can't resolve database connection: %s" db-spec)))
   (keyword? db-spec)
   (connection-url db-spec)
   (string? db-spec)
   db-spec
   :else db-spec))

(defn-memo resolve-connection-pool
  "Resolve the database connection pool for `db-spec`."
  [db-spec] (make-connection-pool db-spec))

(defmacro with-connection
  "Evaluates body in the context of a connection to the database
  `name`. The connection spec for `name` is looked up via environ."
  [db-spec & body]
  `(jdbc/with-naming-strategy *naming-strategy*
     (jdbc/with-connection (resolve-connection ~db-spec)
       ~@body)))

(defmacro with-connection-pool
  "Evaluates body in the context of a connection to the database
  `name`. The connection spec for `name` is looked up via environ."
  [db-spec & body]
  `(jdbc/with-naming-strategy *naming-strategy*
     (jdbc/with-connection (resolve-connection-pool ~db-spec)
       ~@body)))

(defn wrap-connection
  "Returns a Ring handler with an open connection to the `db-spec`
  database."
  [handler db-spec]
  (fn [request]
    (with-connection db-spec
      (handler request))))

(defn wrap-connection-pool
  "Returns a Ring handler with an open connection from a C3P0 pool to
  the `db-spec` database."
  [handler db-spec]
  (fn [request]
    (with-connection-pool db-spec
      (handler request))))
