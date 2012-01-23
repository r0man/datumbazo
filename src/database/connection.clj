(ns database.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as internal])
  (:use [inflections.core :only (dasherize underscore)]
        [leiningen.env.core :only (current-environment *current*)]))

(def ^:dynamic *pools* (atom {}))

(defn db-spec
  "Returns the database spec by name of the environment."
  [environment & [db]]
  (get (:databases environment) (or db :default)
       (if (or (= db :default) (nil? db))
         (:database environment))))

(defn make-connection-pool
  "Make a connection pool from the database spec."
  [db-spec]
  (if db-spec
    (doto (ComboPooledDataSource.)
      (.setDriverClass (:classname db-spec))
      (.setJdbcUrl (str "jdbc:" (:subprotocol db-spec) ":" (:subname db-spec)))
      (.setUser (:user db-spec))
      (.setPassword (:password db-spec))
      (.setMaxIdleTimeExcessConnections (* 30 60))
      (.setMaxIdleTime (* 3 60 60)))))

(defn current-spec
  "Returns the database spec of the current environment."
  [& [db]] (db-spec (current-environment) db))

(defn current-pool
  "Returns the connection pool of the current environment."
  [& [db]]
  (let [db (or db :default)]
    (get-in
     @*pools* [*current* db]
     (when-let [pool (make-connection-pool (current-spec db))]
       (swap! *pools* assoc-in [*current* db] pool)
       pool))))

(defmacro with-connection
  "Evaluates body in the context of a new connection to a database
  then closes the connection."
  [db & body] `(jdbc/with-connection (current-spec ~db) ~@body))

(defmacro with-connection-pool
  "Evaluates body in the context of a pooled connection to the database."
  [db & body] `(jdbc/with-connection {:datasource (current-pool ~db)} ~@body))

(alter-var-root #'internal/*as-str* (constantly underscore))
(alter-var-root #'internal/*as-key* (constantly (comp keyword dasherize)))
