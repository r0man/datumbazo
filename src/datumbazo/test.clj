(ns datumbazo.test
  (:require [datumbazo.core :refer [with-connection]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest]]
            [environ.core :refer [env]]))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-connection :test-db
       (jdbc/transaction
        (jdbc/set-rollback-only)
        ~@body))))
