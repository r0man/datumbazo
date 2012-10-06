(ns database.test
  (:require [database.connection :refer [with-connection]]
            [clojure.test :refer [deftest]]))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-connection :test-db
       (jdbc/transaction
        (jdbc/set-rollback-only)
        ~@body))))