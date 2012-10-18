(ns database.sql.test.compiler
  (:use clojure.test
        database.sql.compiler))

(deftest test-compile-sql
  (are [ast expected]
       (is (= expected (compile-sql ast)))
       {:op :nil}
       ["NULL"]
       {:op :number :form 1}
       ["1"]
       {:op :number :form 3.14}
       ["3.14"]
       {:op :string :form "Europe"}
       ["?" "Europe"]
       {:op :keyword :form :continents.id}
       ["continents.id"]
       {:op :expr-list :children [{:op :number :form 1}]}
       ["1"]
       {:op :expr-list :children [{:op :string :form "x"}]}
       ["?" "x"]
       {:op :expr-list :children [{:op :number :form 1} {:op :string :form "x"}]}
       ["1, ?" "x"]
       {:op :fn :form 'max :children [{:op :keyword :form :created-at}]}
       ["max(created-at)"]
       {:op :fn :form 'greatest :children [{:op :number :form 1} {:op :number :form 2}]}
       ["greatest(1, 2)"]
       {:op :fn :form 'ST_AsText :children [{:op :fn :form 'ST_Centroid :children [{:op :string :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}
       ["ST_AsText(ST_Centroid(?))" "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"]))
