(ns datumbazo.driver.clojure-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer [create-table column select with-db]]
            [datumbazo.driver.core :refer :all]
            [datumbazo.test :refer :all]
            [datumbazo.driver.core :as driver])
  (:import java.sql.Connection))

(deftest test-begin
  (with-db [db db {:backend 'clojure.java.jdbc}]
    (is (.getAutoCommit (connection db)))
    (let [db (begin db)]
      (is (not (.getAutoCommit (connection db))))
      (is (= (.getTransactionIsolation (connection db))))
      (is @(select db [1])))))

(deftest test-connection
  (with-db [db db {:backend 'clojure.java.jdbc}]
    (is (instance? Connection (connection db)))))

(deftest test-commit
  (with-db [db db {:backend 'clojure.java.jdbc}]
    (let [db (begin db)]
      @(select db [1])
      (let [db (commit db)]
        (is db)))))

(deftest test-rollback
  (with-db [db db {:backend 'clojure.java.jdbc}]
    (is (not (:rollback db)))
    (let [db (begin db)]
      @(create-table db :test-rollback
         (column :id :integer))
      (rollback! db)
      (is @(:rollback db))
      (commit db))))
