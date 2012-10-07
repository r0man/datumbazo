(ns database.test
  (:require [database.connection :refer [with-connection-pool]]
            [clojure.test :refer [deftest]]))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-connection-pool :test-db
       (jdbc/transaction
        (jdbc/set-rollback-only)
        ~@body))))