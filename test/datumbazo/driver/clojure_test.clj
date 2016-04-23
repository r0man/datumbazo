(ns datumbazo.driver.clojure-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer :all]
            [datumbazo.driver.core :as d]
            [datumbazo.meta :as meta]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  'clojure.java.jdbc)

(deftest test-begin
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (.getAutoCommit (d/connection db)))
      (let [db (d/begin db)]
        (is (not (.getAutoCommit (d/connection db))))
        (is (= (.getTransactionIsolation (d/connection db))
               Connection/TRANSACTION_READ_COMMITTED))
        (is @(select db [1]))))))

(deftest test-connection
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-connection-rollback
  (with-db [db db {:backend backend}]
    (with-connection [db db {:rollback? true}]
      (is (instance? Connection (d/connection db)))
      (is @(:rollback db))
      (is (not (.getAutoCommit (d/connection db))))
      (is (= (.getTransactionIsolation (d/connection db))
             Connection/TRANSACTION_READ_COMMITTED))
      @(create-table db :test-connection-rollback
         (column :id :integer)))
    (with-connection [db db]
      (is (empty? (meta/tables db :name :test-connection-rollback))))))

(deftest test-connection-with-bonecp
  (with-db [db (assoc db :pool :bonecp) {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-connection-with-c3p0
  (with-db [db (assoc db :pool :c3p0) {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-connection-with-hikaricp
  (with-db [db (assoc db :pool :hikaricp) {:backend backend}]
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-commit
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (let [db (d/begin db)]
        @(select db [1])
        (let [db (d/commit db)]
          (is db))))))

(deftest test-rollback
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (not (:rollback db)))
      (let [db (d/begin db)]
        @(create-table db :test-rollback
           (column :id :integer))
        (is (= (d/rollback! db) db))
        (is @(:rollback db))
        (d/commit db)))))
