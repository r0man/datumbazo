(ns database.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank?]]
            [database.util :refer [parse-url parse-integer]]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]))

(def ^:dynamic *connection* nil)

(def ^:dynamic *naming-strategy*
  {:entity underscore :keyword dasherize})

(def ^:dynamic *pool-params*
  {:max-pool-size 15
   :max-idle-time (* 3 60 60)
   :max-idle-time-excess-connections (* 30 60)})

(defn resolve-connection
  [db-spec]
  (if (keyword? db-spec)
    (or (env db-spec)
        (throw (IllegalArgumentException. (format "Can't resolve connection spec via environ: %s" db-spec))))
    db-spec))

(defn make-connection-pool
  "Make a C3P0 database connection pool."
  [url]
  (if-let [db-spec (parse-url url)]
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

(defmacro with-connection
  "Evaluates body in the context of a connection to the database
  `name`. The connection spec for `name` is looked up via environ."
  [name & body]
  `(jdbc/with-naming-strategy *naming-strategy*
     (jdbc/with-connection (resolve-connection ~name)
       ~@body)))

(defn wrap-connection
  "Returns a Ring handler with that has a connection to the `db-spec`."
  [handler db-spec]
  (fn [request]
    (with-connection db-spec
      (handler request))))
