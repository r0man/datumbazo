(ns datumbazo.driver.funcool-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer :all]
            [datumbazo.meta :as meta]
            [datumbazo.driver.core :as d]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  'jdbc.core)

(deftest test-begin
  (with-db [db db {:backend backend}]
    (with-connection [db db]
      (is (.getAutoCommit (d/connection db)))
      (is (not (-> db :connection meta :transaction)))
      (let [db (d/begin db)]
        (is (not (.getAutoCommit (d/connection db))))
        (is (-> db :connection meta :transaction))
        @(select db [1])))))

(deftest test-connection
  (with-db [db db {:backend backend}]
    (is (nil? (d/connection db)))
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-connection-rollback
  (with-db [db db {:backend backend}]
    (with-connection [db db {:rollback? true}]
      (is (instance? Connection (d/connection db)))
      (is (not (.getAutoCommit (d/connection db))))
      (is (-> db :connection meta :transaction))
      (is (= (.getTransactionIsolation (d/connection db))
             Connection/TRANSACTION_READ_COMMITTED))
      @(create-table db :test-connection-rollback
         (column :id :integer)))
    (with-connection [db db]
      (is (empty? (meta/tables db :name :test-connection-rollback))))))

(deftest test-connection-with-bonecp
  (with-db [db (assoc db :pool :bonecp) {:backend backend}]
    (is (nil? (d/connection db)))
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-connection-with-c3p0
  (with-db [db (assoc db :pool :c3p0) {:backend backend}]
    (is (nil? (d/connection db)))
    (with-connection [db db]
      (is (instance? Connection (d/connection db))))))

(deftest test-connection-with-hikaricp
  (with-db [db (assoc db :pool :hikaricp) {:backend backend}]
    (is (nil? (d/connection db)))
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
      (is (not (-> db :connection meta :rollback)))
      (let [db (d/begin db)]
        @(create-table db :test-rollback
           (column :id :integer))
        (is (= (d/rollback! db) db))
        (is (-> db :connection meta :rollback deref))
        (d/commit db)))))
