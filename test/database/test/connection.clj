(ns database.test.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.connection
        clojure.test))

(deftest test-make-connection-pool
  (let [pool (make-connection-pool "postgresql://localhost/test")]
    (is (map? pool))
    (let [datasource (:datasource pool)]
      (is (instance? ComboPooledDataSource datasource))
      (is (= "jdbc:postgresql://localhost/test" (.getJdbcUrl datasource)))
      (is (= 15 (.getMaxPoolSize datasource)))
      (is (= 10800 (.getMaxIdleTime datasource)))
      (is (= 1800 (.getMaxIdleTimeExcessConnections datasource))))
    (with-connection pool
      (is (jdbc/connection)))))

(deftest test-with-connection
  (with-connection :bs-database
    (is (jdbc/connection)))
  (is (thrown? IllegalArgumentException (with-connection :unknown))))

(deftest test-wrap-connection
  ((wrap-connection
    (fn [request] (is (jdbc/connection))) :bs-database)
   {}))