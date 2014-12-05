(ns datumbazo.connection-test
  (:import [com.jolbox.bonecp BoneCPDataSource ConnectionHandle]
           com.mchange.v2.c3p0.ComboPooledDataSource
           com.mchange.v2.c3p0.impl.NewProxyConnection
           java.sql.Connection)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [datumbazo.connection :refer :all]
            [datumbazo.core :refer [as select]]
            [datumbazo.test :refer :all]))

(deftest test-query
  (with-test-db [db]
    (is (= [[1]] (map vals (jdbc/query db ["SELECT 1"]))))))

(deftest test-sql-str
  (with-test-db [db]
    (is (= "SELECT 1, 'a'" (sql-str (select db [1 "a"]))))))

(deftest test-select-pool-bonecp
  (with-test-db [db "bonecp:postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (nil? (:connection db)))
    (is (instance? BoneCPDataSource (:datasource db)))
    (is (= @(select db [(as 1 :a)])
           [{:a 1}]))))

(deftest test-select-pool-c3p0
  (with-test-db [db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (nil? (:connection db)))
    (is (instance? ComboPooledDataSource (:datasource db)))
    (is (= @(select db [(as 1 :a)])
           [{:a 1}]))))

(deftest test-select-pool-mysql
  (with-test-db [db "mysql://tiger:scotch@localhost/datumbazo"]
    (is (instance? Connection (:connection db)))
    (is (= @(select db [(as 1 :a)])
           [{:a 1}]))))

(deftest test-select-postgresql
  (with-test-db [db "postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (instance? Connection (:connection db)))
    (is (= @(select db [(as 1 :a)])
           [{:a 1}]))))

(deftest test-with-connection
  (with-test-db [db "postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (with-connection [connection db]
      (is (instance? Connection connection))))
  (with-test-db [db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (with-connection [connection db]
      (is (instance? Connection connection)))))
