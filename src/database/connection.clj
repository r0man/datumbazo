(ns database.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use [inflections.core :only (dasherize)]
        [leiningen.env.core :only (current-environment)]))

(defn db-spec
  "Returns the database spec of the environment."
  [environment & [db-name]]
  (if db-name
    (db-name (:databases environment))
    (or (:database environment)
        (:default (:databases environment)))))

(defn current-db-spec
  "Returns the database spec for the current environment."
  [& [db-name]] (db-spec (current-environment) db-name))

(defn make-pool
  "Make a connection pool."
  [db-spec]
  (doto (ComboPooledDataSource.)
    (.setDriverClass (:classname db-spec))
    (.setJdbcUrl (str "jdbc:" (:subprotocol db-spec) ":" (:subname db-spec)))
    (.setUser (:user db-spec))
    (.setPassword (:password db-spec))
    (.setMaxIdleTimeExcessConnections (* 30 60))
    (.setMaxIdleTime (* 3 60 60))))

(alter-var-root #'internal/*as-key* (constantly dasherize))
