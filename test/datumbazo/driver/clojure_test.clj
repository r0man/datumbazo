(ns datumbazo.driver.clojure-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer :all]
            [datumbazo.driver.core :refer :all]
            [datumbazo.test :refer :all]
            [datumbazo.meta :as meta])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  'clojure.java.jdbc)

(deftest test-begin
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (.getAutoCommit (connection db)))
      (let [db (begin db)]
        (is (not (.getAutoCommit (connection db))))
        (is (= (.getTransactionIsolation (connection db))
               Connection/TRANSACTION_READ_COMMITTED))
        (is @(select db [1]))))))

(deftest test-connection
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-connection-rollback
  (with-db [db db {:backend backend}]
    (with-connection [db db {:rollback? true}]
      (is (instance? Connection (connection db)))
      (is @(:rollback db))
      (is (not (.getAutoCommit (connection db))))
      (is (= (.getTransactionIsolation (connection db))
             Connection/TRANSACTION_READ_COMMITTED))
      @(create-table db :test-connection-rollback
         (column :id :integer)))
    (with-connection [db db]
      (is (empty? (meta/tables db :name :test-connection-rollback))))))

(deftest test-connection-with-bonecp
  (with-db [db (assoc db :pool :bonecp) {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-connection-with-c3p0
  (with-db [db (assoc db :pool :c3p0) {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-connection-with-hikaricp
  (with-db [db (assoc db :pool :hikaricp) {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-commit
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (let [db (begin db)]
        @(select db [1])
        (let [db (commit db)]
          (is db))))))

(deftest test-rollback
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (not (:rollback db)))
      (let [db (begin db)]
        @(create-table db :test-rollback
           (column :id :integer))
        (is (= (rollback! db) db))
        (is @(:rollback db))
        (commit db)))))
