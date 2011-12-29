(ns database.test.connection
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource C3P0ProxyConnection)
           java.sql.Connection)
  (:require [clojure.java.jdbc :as jdbc])
  (:use [leiningen.env.core :only (with-environment)]
        clojure.test
        database.connection
        database.test))

(deftest test-current-spec
  (load-environments)
  (let [spec (current-spec)]
    (is (= "org.postgresql.Driver" (:classname spec)))
    (is (= "database" (:password spec)))
    (is (= "//localhost:5432/database_development" (:subname spec)))
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "database" (:user spec))))
  (with-environment :test
    (let [spec (current-spec)]
      (is (= "org.postgresql.Driver" (:classname spec)))
      (is (= "database" (:password spec)))
      (is (= "//localhost:5432/database_test" (:subname spec)))
      (is (= "postgresql" (:subprotocol spec)))
      (is (= "database" (:user spec))))))

(deftest test-current-pool
  (load-environments)
  (let [pool (current-pool)]
    (is (instance? ComboPooledDataSource pool))
    (with-environment :test
      (is (instance? ComboPooledDataSource (current-pool)))
      (is (not (= pool (current-pool)))))))

(deftest test-db-spec
  (let [spec (current-spec)]
    (is (= spec (db-spec {:database spec})))
    (is (= spec (db-spec {:databases {:default spec}})))
    (is (= spec (db-spec {:databases {:analytic spec}} :analytic)))))

(deftest test-make-connection-pool
  (let [pool (make-connection-pool (current-spec))]
    (is (instance? ComboPooledDataSource pool))
    (is (= "jdbc:postgresql://localhost:5432/database_development" (.getJdbcUrl pool)))
    (is (= "database" (.getUser pool)))
    (is (= "database" (.getPassword pool)))))

(deftest test-with-connection
  (with-connection :default
    (is (instance? Connection (jdbc/find-connection)))))

(deftest test-with-connection-pool
  (with-connection-pool :default
    (let [connection (jdbc/find-connection)]
      (is (instance? Connection connection))
      (is (instance? C3P0ProxyConnection connection)))))
