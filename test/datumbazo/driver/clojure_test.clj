(ns datumbazo.driver.clojure-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  'clojure.java.jdbc)

(deftest test-begin
  (sql/with-db [db db {:backend backend}]
    (is (thrown? AssertionError (sql/begin db)))
    (sql/with-connection [db db]
      (is (.getAutoCommit (sql/connection db)))
      (let [db (sql/begin db)]
        (is (not (.getAutoCommit (sql/connection db))))
        (is (= (.getTransactionIsolation (sql/connection db))
               Connection/TRANSACTION_READ_COMMITTED))
        (is @(sql/select db [1]))))))

(deftest test-connect
  (sql/with-db [db db {:backend backend}]
    (let [db (sql/connect db)]
      (is (instance? Connection (sql/connection db)))
      (sql/disconnect db))))

(deftest test-connection
  (sql/with-db [db db {:backend backend}]
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-bonecp
  (sql/with-db [db (assoc db :pool :bonecp) {:backend backend}]
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-c3p0
  (sql/with-db [db (assoc db :pool :c3p0) {:backend backend}]
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-hikaricp
  (sql/with-db [db (assoc db :pool :hikaricp) {:backend backend}]
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-commit
  (sql/with-db [db db {:backend backend}]
    (sql/with-connection [db db]
      (let [db (sql/begin db)]
        @(sql/select db [1])
        (let [db (sql/commit db)]
          (is db))))))

(deftest test-disconnect
  (sql/with-db [db db {:backend backend}]
    (let [db (sql/disconnect (sql/connect db))]
      (is (nil? (sql/connection db))))))

(deftest test-rollback
  (sql/with-db [db db {:backend backend}]
    (sql/with-connection [db db]
      (is (not (:rollback db)))
      (let [db (sql/begin db)]
        @(sql/create-table db :test-rollback
           (sql/column :id :integer))
        (is (= (sql/rollback db) db))
        (is (-> db :driver :rollback deref))
        (sql/commit db)))))

(deftest test-test-db
  (sql/with-db [db db {:backend backend}]
    @(sql/drop-table db [:test]
       (sql/if-exists true)))
  (sql/with-db [db db {:backend backend :test? true}]
    @(sql/create-table db :test
       (sql/column :id :uuid)))
  (sql/with-db [db db {:backend backend}]
    (is (empty? @(sql/select db [:*]
                   (sql/from :information_schema.tables)
                   (sql/where '(= :table_name "test")))))))
