(ns datumbazo.driver.funcool-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  'jdbc.core)

(deftest test-begin
  (sql/with-db [db db {:backend backend}]
    (sql/with-connection [db db]
      (is (.getAutoCommit (sql/connection db)))
      (is (not (-> db :connection meta :transaction)))
      (let [db (sql/begin db)]
        (is (not (.getAutoCommit (sql/connection db))))
        (is (-> db :driver :connection meta :transaction))
        @(sql/select db [1])))))

(deftest test-connect
  (sql/with-db [db db {:backend backend}]
    (let [db (sql/connect db)]
      (is (instance? Connection (sql/connection db)))
      (sql/disconnect db))))

(deftest test-connection
  (sql/with-db [db db {:backend backend}]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-bonecp
  (sql/with-db [db (assoc db :pool :bonecp) {:backend backend}]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-c3p0
  (sql/with-db [db (assoc db :pool :c3p0) {:backend backend}]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-hikaricp
  (sql/with-db [db (assoc db :pool :hikaricp) {:backend backend}]
    (is (nil? (sql/connection db)))
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
      (is (not (-> db :driver :connection meta :rollback)))
      (let [db (sql/begin db)]
        (is (-> db :driver :connection meta :transaction))
        (is (not (-> db :driver :connection meta :rollback deref)))
        @(sql/create-table db :test-rollback
           (sql/column :id :integer))
        (is (= (sql/rollback db) db))
        (is (-> db :driver :connection meta :rollback deref))))))

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
