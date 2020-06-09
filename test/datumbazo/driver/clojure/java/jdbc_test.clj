(ns datumbazo.driver.clojure.java.jdbc-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]
            [sqlingvo.util :as util])
  (:import java.sql.Connection))

(def backend
  "The name of the current database backend."
  :clojure.java.jdbc)

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

(deftest test-with-transaction-rollback-manual
  (sql/with-db [db db {:backend backend}]
    (sql/with-transaction [db db]
      (try @(sql/create-table db :my-table
              (sql/column :id :serial))

           (try (sql/with-savepoint [db db]
                  @(sql/insert db :my-table [:id]
                     (sql/values [[1]])
                     (sql/returning :*))
                  (throw (ex-info "BOOM" {})))
                (catch Exception _))

           (is (empty? @(sql/select db [:*]
                          (sql/from :my-table))))

           (finally (sql/rollback! db))))

    (is (empty? @(sql/select db [:table_name]
                   (sql/from :information_schema.tables)
                   (sql/where `(and (= :table_schema "public")
                                    (= :table_name "my-table"))))))))

(deftest test-with-transaction-rollback-option
  (sql/with-db [db db {:backend backend}]
    (sql/with-transaction [db db {:rollback-only true}]

      @(sql/create-table db :my-table
         (sql/column :id :serial))

      (try (sql/with-savepoint [db db]
             @(sql/insert db :my-table [:id]
                (sql/values [[1]])
                (sql/returning :*))
             (throw (ex-info "BOOM" {})))
           (catch Exception _))

      (is (empty? @(sql/select db [:*]
                     (sql/from :my-table)))))

    (is (empty? @(sql/select db [:table_name]
                   (sql/from :information_schema.tables)
                   (sql/where `(and (= :table_schema "public")
                                    (= :table_name "my-table"))))))))
