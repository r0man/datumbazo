(ns datumbazo.driver.test-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer :all]
            [datumbazo.meta :as meta]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-begin
  (with-drivers [db db]
    (is (thrown? AssertionError (begin db)))
    (with-connection [db db]
      (let [db (begin db)]
        (is (not (.getAutoCommit (connection db))))
        @(select db [1])))))

(deftest test-connect
  (with-drivers [db db]
    (let [db (connect db)]
      (is (instance? Connection (connection db))))))

(deftest test-connection
  (with-drivers [db db]
    (is (nil? (connection db)))
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-connection-with-bonecp
  (with-drivers [db db]
    (is (nil? (connection db)))
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-connection-with-c3p0
  (with-drivers [db db]
    (is (nil? (connection db)))
    (with-connection [db db]
      (is (instance? Connection (connection db))))))

(deftest test-connection-with-hikaricp
  (with-drivers [db db]
    (is (nil? (connection db)))
    (with-connection [db db]
      (is (instance? Connection (connection db)))
      @(select db [1]))))

(deftest test-commit
  (with-drivers [db db]
    (is (thrown? AssertionError (commit db)))
    (with-connection [db db]
      (let [db (begin db)]
        @(select db [1])
        (let [db (commit db)]
          (is db))))))

(deftest test-disconnect
  (with-drivers [db db]
    (is (thrown? AssertionError (disconnect db)))
    (let [db (disconnect (connect db))]
      (is (nil? (connection db))))))

(deftest test-fetch
  (with-drivers [db db]
    (is (thrown? AssertionError @(select db [1])))
    (with-connection [db db]
      (is (= @(select db [1])
             [{:?column? 1}])))))

(deftest test-execute
  (with-drivers [db db]
    (is (thrown? AssertionError
                 @(create-table db :test-execute
                    (column :id :serial))))
    (with-connection [db db]
      (is (= @(create-table db :test-execute
                (column :id :serial))
             [{:count 0}])))))

(deftest test-rollback
  (with-drivers [db db]
    (is (thrown? AssertionError (rollback db)))
    (with-connection [db db]
      (let [db (begin db)]
        @(create-table db :test-rollback
           (column :id :integer))
        (is (= (rollback db) db))
        (commit db)))))

(deftest test-with-transaction
  (with-drivers [db db]
    (is (thrown? AssertionError (with-transaction [db db])))
    (with-connection [db db]
      (with-transaction [db db]
        @(create-table db :with-transaction
           (column :id :integer))
        (rollback db)))
    (with-connection [db db]
      (is (empty? (meta/tables db {:name :with-transaction}))))))
