(ns datumbazo.connection-test
  (:require [clojure.test :refer :all]
            [datumbazo.connection :refer :all]
            [datumbazo.core :refer [as select]]
            [datumbazo.test :refer :all])
  (:import com.jolbox.bonecp.BoneCPDataSource
           com.mchange.v2.c3p0.ComboPooledDataSource
           java.sql.Connection))

(deftest test-sql-str
  (with-backends [db]
    (is (= "SELECT 1, 'a'" (sql-str (select db [1 "a"]))))))

(deftest test-select-pool-bonecp
  (with-test-db [db "bonecp:postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (instance? Connection (:connection db)))
    (is (instance? BoneCPDataSource (:datasource db)))
    (is (= @(select db [(as 1 :a)])
           [{:a 1}]))))

(deftest test-select-pool-c3p0
  (with-test-db [db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo"]
    (is (instance? Connection (:connection db)))
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
