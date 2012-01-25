(ns database.test
  (:require [clojure.java.jdbc :as jdbc]
            [migrate.core :as migrate])
  (:use [inflections.core :only (dasherize underscore)]
        clojure.test
        korma.db
        korma.core))

(defdb test-database
  (postgres
   {:db "database_test"
    :host "localhost"
    :port "5432"
    :user "database"
    :password "database"
    :naming {:keys (comp keyword dasherize) :fields (comp name underscore)}}))

(defmacro with-test-database
  "Evaluate `body` with test-database set as default connection."
  [& body]
  `(do (default-connection test-database)
       ~@body))

(defmacro with-test-connection
  "Evaluate `body` in the context of a test connection."
  [& body]
  `(with-test-database
     (jdbc/with-connection (get-connection test-database)
       ~@body)))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-test-connection
       (migrate/run)
       (jdbc/transaction
        (try ~@body (finally (rollback)))))))
