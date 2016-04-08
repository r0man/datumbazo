(ns datumbazo.driver.funcool-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer [create-table column select with-db]]
            [datumbazo.driver.core :refer :all]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-begin
  (with-db [db db {:backend 'jdbc.core}]
    (is (.getAutoCommit (connection db)))
    (is (not (-> db :connection meta :transaction)))
    (let [db (begin db)]
      (is (not (.getAutoCommit (connection db))))
      (is (-> db :connection meta :transaction))
      @(select db [1]))))

(deftest test-connection
  (with-db [db db {:backend 'jdbc.core}]
    (is (instance? Connection (connection db)))))

(deftest test-commit
  (with-db [db db {:backend 'jdbc.core}]
    (let [db (begin db)]
      @(select db [1])
      (let [db (commit db)]
        (is db)))))

(deftest test-rollback
  (with-db [db db {:backend 'jdbc.core}]
    (is (not (-> db :connection meta :rollback)))
    (let [db (begin db)]
      @(create-table db :test-rollback
         (column :id :integer))
      (rollback! db)
      (is (-> db :connection meta :rollback deref))
      (commit db))))
