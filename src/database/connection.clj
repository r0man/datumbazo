(ns database.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use [inflections.core :only (dasherize)]
        [leiningen.env.core :only (current-environment)]))

(defn db-spec
  "Returns the database spec of the environment."
  [environment & [db]]
  (if db
    (db (:databases environment))
    (or (:database environment)
        (:default (:databases environment)))))

(defn current-spec
  "Returns the database spec for the current environment."
  [& [db]] (db-spec (current-environment) db))

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
