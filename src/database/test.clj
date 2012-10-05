(ns database.test)

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (with-database :database-clj-test-db
       (migrate/run)
       (jdbc/transaction
        (jdbc/set-rollback-only)
        ~@body))))