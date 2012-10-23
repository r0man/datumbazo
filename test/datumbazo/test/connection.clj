(ns datumbazo.test.connection
  (:import com.mchange.v2.c3p0.ComboPooledDataSource
           com.mchange.v2.c3p0.impl.NewProxyConnection
           java.sql.Connection)
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.connection
        datumbazo.util
        clojure.test))

(deftest test-connection-url
  (is (thrown? IllegalArgumentException (connection-url :unknown-db)))
  (is (= "postgresql://tiger:scotch@localhost/test" (connection-url :test-db))))

(deftest test-connection-spec
  (is (thrown? IllegalArgumentException (connection-spec :unknown-db)))
  (is (map? (connection-spec :test-db))))

(deftest test-make-connection-pool
  (let [pool (make-connection-pool :test-db)]
    (is (map? pool))
    (let [datasource (:datasource pool)]
      (is (instance? ComboPooledDataSource datasource))
      (is (= "jdbc:postgresql://localhost/test" (.getJdbcUrl datasource)))
      (is (= 15 (.getMaxPoolSize datasource)))
      (is (= 10800 (.getMaxIdleTime datasource)))
      (is (= 1800 (.getMaxIdleTimeExcessConnections datasource))))
    (with-connection pool
      (is (jdbc/connection))))
  (let [pool (make-connection-pool "postgresql://localhost/test")]
    (is (map? pool))
    (let [datasource (:datasource pool)]
      (is (instance? ComboPooledDataSource datasource))
      (is (= "jdbc:postgresql://localhost/test" (.getJdbcUrl datasource)))
      (is (= 15 (.getMaxPoolSize datasource)))
      (is (= 10800 (.getMaxIdleTime datasource)))
      (is (= 1800 (.getMaxIdleTimeExcessConnections datasource))))
    (with-connection pool
      (is (jdbc/connection))))
  (let [pool (make-connection-pool (parse-url "postgresql://localhost/test"))]
    (is (map? pool))
    (let [datasource (:datasource pool)]
      (is (instance? ComboPooledDataSource datasource))
      (is (= "jdbc:postgresql://localhost/test" (.getJdbcUrl datasource)))
      (is (= 15 (.getMaxPoolSize datasource)))
      (is (= 10800 (.getMaxIdleTime datasource)))
      (is (= 1800 (.getMaxIdleTimeExcessConnections datasource))))
    (with-connection pool
      (is (jdbc/connection)))))

(deftest test-resolve-connection
  (is (= (connection-url :test-db)
         (resolve-connection :test-db)))
  (is (= (connection-url :test-db)
         (resolve-connection (connection-url :test-db)))))

(deftest test-resolve-connection-pool
  (let [pool (resolve-connection-pool :test-db)]
    (is (map? pool))
    (is (= "jdbc:postgresql://localhost/test" (.getJdbcUrl (:datasource pool))))
    (is (= (:datasource pool) (:datasource (resolve-connection-pool :test-db))))))

(deftest test-with-connection
  (with-connection :test-db
    (is (instance? Connection (jdbc/connection))))
  (is (thrown? IllegalArgumentException (with-connection :unknown))))

(deftest test-with-connection-pool
  (with-connection-pool :test-db
    (is (instance? NewProxyConnection (jdbc/connection)))))

(deftest test-wrap-connection
  ((wrap-connection
    (fn [request]
      (is (instance? Connection (jdbc/connection))))
    :test-db)
   {}))

(deftest test-wrap-connection-pool
  ((wrap-connection-pool
    (fn [request]
      (is (instance? NewProxyConnection (jdbc/connection))))
    :test-db)
   {}))
