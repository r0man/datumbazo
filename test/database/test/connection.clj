(ns database.test.connection
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.fixtures
        database.test
        korma.core
        korma.db))

(deftest test-jdbc-connection
  (let [connection (jdbc-connection :database-clj-test-db)]
    (is (map? connection))
    (is (instance? javax.sql.DataSource (:datasource connection)))))

(deftest test-korma-connection
  (let [connection (korma-connection :database-clj-test-db)]
    (is (= (jdbc-connection :database-clj-test-db)
           @(:pool connection)))
    (is (= ["\"" "\""] (:delimiters (:options connection))))))

(deftest test-with-database
  (with-database :database-clj-test-db
    (testing "jdbc connection"
      (is (instance? java.sql.Connection (jdbc/connection)))
      (is (= 1 (:n (first (jdbc/with-query-results rs ["SELECT 1 AS n"] (doall rs)))))))
    (testing "korma connection"
      (is (= (korma-connection :database-clj-test-db) @_default))
      (is (select :schema_migrations)))))
