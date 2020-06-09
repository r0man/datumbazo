(ns datumbazo.driver.jdbc.core-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]
            [sqlingvo.util :as util])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  :jdbc.core)

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

(deftest test-disconnect
  (sql/with-db [db db {:backend backend}]
    (let [db (sql/disconnect (sql/connect db))]
      (is (nil? (sql/connection db))))))

(deftest test-naming-strategy
  (sql/with-db [db db {:backend backend
                       :sql-keyword util/sql-keyword-hyphenate
                       :sql-name util/sql-name-underscore}]
    (is (= @(sql/select db [:*]
              (sql/from :information-schema.tables)
              (sql/where '(= :table-name "pg_statistic")))
           [{:commit-action nil,
             :reference-generation nil,
             :is-typed "NO",
             :is-insertable-into "YES",
             :table-catalog "datumbazo",
             :user-defined-type-schema nil,
             :user-defined-type-name nil,
             :user-defined-type-catalog nil,
             :table-schema "pg_catalog",
             :self-referencing-column-name nil,
             :table-name "pg_statistic",
             :table-type "BASE TABLE"}]))))

(deftest test-transact
  (sql/with-db [db db {:backend backend}]
    (sql/with-connection [db db]
      (sql/transact db #(is (sql/db? %)))
      (sql/transact db #(is (= (sql/connection db) (sql/connection %)))))))
