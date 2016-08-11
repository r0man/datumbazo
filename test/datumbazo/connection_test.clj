(ns datumbazo.connection-test
  (:require [clojure.test :refer :all]
            [datumbazo.connection :refer :all]
            [datumbazo.core :refer [check create-table inherits select sql]]
            [datumbazo.db :refer [with-db]]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-with-connection
  (with-db [db (:postgresql connections)]
    (with-connection [db db]
      (let [connection (connection db)]
        (is (instance? Connection connection))))))

(deftest test-sql-str
  (with-backends [db]
    (is (= (sql-str (select db [1 "a"]))
           "SELECT 1, 'a'"))
    (is (= (sql-str (create-table db :measurement-y2006m02
                      (check '(>= :logdate "2006-02-01"))
                      (check '(< :logdate "2006-03-01"))
                      (inherits :measurement)))
           (str "CREATE TABLE \"measurement-y2006m02\" ("
                "CHECK (\"logdate\" >= '2006-02-01'), "
                "CHECK (\"logdate\" < '2006-03-01')) "
                "INHERITS (\"measurement\")")))))

(deftest test-execute-sql-query
  (with-backends [db]
    (is (= (execute-sql-query db (sql (select db [1])))
           [{:?column? 1}]))))
