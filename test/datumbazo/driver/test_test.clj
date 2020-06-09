(ns datumbazo.driver.test-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.core :as sql]
            [datumbazo.meta :as meta]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-connect
  (with-drivers [db (assoc db :rollback-only true)]
    (is (= false (-> db :driver :connected?)))
    (let [db (sql/connect db)]
      (is (= true (-> db :driver :connected?)))
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection
  (with-drivers [db (assoc db :rollback-only true)]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (= true (-> db :driver :connected?)))
      (is (instance? Connection (sql/connection db))))))

(deftest test-disconnect
  (with-drivers [db (assoc db :rollback-only true)]
    (is (thrown? AssertionError (sql/disconnect db)))
    (let [db (sql/disconnect (sql/connect db))]
      (is (= false (-> db :driver :connected?)))
      (is (nil? (sql/connection db))))))

(deftest testexecute-all
  (with-drivers [db (assoc db :rollback-only true)]
    (is (= @(sql/select db [1])
           [{:?column? 1}]))))

(deftest test-execute-one
  (with-drivers [db db {:rollback-only true}]
    (sql/with-transaction [db db {:rollback-only true}]
      (is (= @(sql/create-table db :test-execute-one
                (sql/column :id :serial))
             [{:count 0}])))))

(deftest test-with-transaction
  (with-drivers [db (assoc db :rollback-only true)]
    (sql/with-transaction [db db]
      (is (instance? Connection (sql/connection db))))
    (sql/with-connection [db db]
      (sql/with-transaction [db db]
        @(sql/create-table db :with-transaction
           (sql/column :id :integer))
        (sql/rollback! db)))
    (sql/with-connection [db db]
      (is (empty? (meta/tables db {:name :with-transaction}))))))
