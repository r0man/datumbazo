(ns datumbazo.test
  (:require [datumbazo.connection :refer [connection-spec connection-pool]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest]]
            [environ.core :refer [env]]
            [sqlingvo.core :refer  [with-rollback select run]]))

(def db (env :test-db))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ^:integration ~name
     (with-open [db# (jdbc/get-connection db)]
       (jdbc/db-transaction
        [~'db (jdbc/add-connection nil db#)]
        (jdbc/db-set-rollback-only! ~'db)
        ~@body))))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ^:integration ~name
     (with-rollback [~'db db]
       ~@body)))

(database-test test-wadada
  (run db (select [1])))

(comment

  ;; leaks connections
  (jdbc/db-transaction
   [t-db "postgresql://tiger:scotch@localhost/datumbazo"]
   t-db)

  (with-open [db (jdbc/get-connection "postgresql://tiger:scotch@localhost/datumbazo")]
    (jdbc/db-transaction
     [t-db (jdbc/add-connection nil db)]
     t-db))

  )