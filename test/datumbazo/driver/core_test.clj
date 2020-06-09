(ns datumbazo.driver.core-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.core :as sql]
            [datumbazo.db :refer [with-db]]
            [datumbazo.driver.core :as driver]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-with-connection
  (with-db [db (:postgresql connections)]
    (driver/with-connection [db db]
      (let [connection (driver/connection db)]
        (is (instance? Connection connection))))))

(deftest test-sql-str
  (with-backends [db]
    (is (= (driver/sql-str (sql/select db [1 "a"]))
           "SELECT 1, 'a'"))
    (is (= (driver/sql-str
            (sql/create-table db :measurement-y2006m02
              (sql/check '(>= :logdate "2006-02-01"))
              (sql/check '(< :logdate "2006-03-01"))
              (sql/inherits :measurement)))
           (str "CREATE TABLE \"measurement-y2006m02\" ("
                "CHECK (\"logdate\" >= '2006-02-01'), "
                "CHECK (\"logdate\" < '2006-03-01')) "
                "INHERITS (\"measurement\")")))))

(deftest test-execute-one-sql-query
  (with-backends [db]
    (is (= (driver/execute-sql-query db (sql/sql (sql/select db [1])))
           [{:?column? 1}]))))

(deftest test-with-transaction-rollback-manual
  (with-drivers [db db]
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
  (with-drivers [db db]
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

(deftest test-create-table
  (with-drivers [db db]
    (sql/with-transaction [db db {:rollback-only true}]
      (is @(sql/create-table db :test-create-table
             (sql/column :id :timestamp)
             (sql/check `(not (= :id ~(java.sql.Date. 0)))))))))
