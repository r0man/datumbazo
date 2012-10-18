(ns database.sql.test.compiler
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.sql.compiler))

(deftest test-compile-sql
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :nil}
       ["NULL"]
       {:op :number :form 1}
       ["1"]
       {:op :keyword :form :continents.created-at}
       ["continents.created-at"]
       {:op :expr-list :children [{:op :number :form 1}]}
       ["1"]
       {:op :expr-list :children [{:op :string :form "x"}]}
       ["?" "x"]
       {:op :expr-list :children [{:op :number :form 1} {:op :string :form "x"}]}
       ["1, ?" "x"]
       {:op :fn :name 'max :children [{:op :keyword :form :created-at}]}
       ["max(created-at)"]
       {:op :fn :name 'greatest :children [{:op :number :form 1} {:op :number :form 2}]}
       ["greatest(1, 2)"]
       {:op :fn :name 'ST_AsText :children [{:op :fn :name 'ST_Centroid :children [{:op :string :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}
       ["ST_AsText(ST_Centroid(?))" "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"])
  (jdbc/with-quoted-identifiers \"
    (is (= ["\"continents\".\"created-at\""]
           (compile-sql {:op :keyword :form :continents.created-at})))))

(deftest test-compile-number
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :number :form 1}
       ["1"]
       {:op :number :form 3.14}
       ["3.14"]
       ))

(deftest test-compile-limit
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :limit :count 1}
       ["LIMIT 1"]
       {:op :limit :count nil}
       ["LIMIT ALL"]))

(deftest test-compile-select
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :select :expressions [] :from-item [{:op :table :name :continents}]}
       ["SELECT * FROM continents"]
       ;; {:op :select :expressions [] :from-item [{:op :table :name :continents :limit 1}]}
       ;; ["SELECT * FROM continents LIMIT 1"]
       ))

(deftest test-compile-string
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :number :form 1}
       ["1"]
       {:op :number :form 3.14}
       ["3.14"]
       ))

(deftest test-compile-offset
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :offset :start 1}
       ["OFFSET 1"]
       {:op :offset :start nil}
       ["OFFSET 0"]))