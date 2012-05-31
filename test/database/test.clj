(ns database.test
  (:require [clojure.java.jdbc :as jdbc]
            [migrate.core :as migrate])
  (:use database.connection
        clojure.test))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-database :database-clj-test-db
       (migrate/run)
       (jdbc/transaction
        (jdbc/set-rollback-only)
        ~@body))))
