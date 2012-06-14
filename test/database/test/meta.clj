(ns database.test.meta
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.meta
        database.test))

(database-test test-version
  (let [version (version (jdbc/connection))]
    (is (string? (:product-name version)))
    (is (number? (:major-version version)))
    (is (number? (:minor-version version)))))

(database-test test-schemas
  (is (schemas (jdbc/connection))))

(database-test test-tables
  (is (tables (jdbc/connection)))
  (tables (jdbc/connection) :table-pattern "x"))
