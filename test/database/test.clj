(ns database.test
  (:require [clojure.java.jdbc :as jdbc]
            [migrate.core :as migrate])
  (:use [inflections.core :only (dasherize underscore)]
        database.connection
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

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-connection test-database
       (migrate/run)
       (jdbc/transaction
        (try ~@body (finally (rollback)))))))
