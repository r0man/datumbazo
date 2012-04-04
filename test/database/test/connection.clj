(ns database.test.connection
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.test
        database.fixtures))

(deftest test-connection-spec
  (let [spec (connection-spec)]
    (is (map? spec))))

(deftest test-connection
  (let [connection (connection)]
    (is (map? connection))
    (is (instance? javax.sql.DataSource (:datasource connection)))))

(database-test test-naming-strategy
  (let [strategy (naming-strategy)]
    (is (fn? (:keys strategy)))
    (is (not (= identity (:keys strategy))))
    (is (fn? (:fields strategy)))
    (is (not (= identity (:fields strategy))))))

(deftest test-with-connection
  (with-connection test-database
    (is (instance? java.sql.Connection (jdbc/connection)))
    (is (= 1 (:n (first (jdbc/with-query-results rs ["SELECT 1 AS n"] (doall rs))))))))

;; (with-connection development-database
;;   (migrate.core/run))
