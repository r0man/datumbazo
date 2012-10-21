(ns datumbazo.sql.test.expr
  (:use clojure.test
        datumbazo.sql.expr))

(deftest test-parse-expr
  (are [expr expected]
       (is (= expected (parse-expr expr)))
       nil
       {:op :nil}
       1
       {:op :constant :form 1}
       1.2
       {:op :constant :form 1.2}
       "Europe"
       {:op :constant :form "Europe"}
       :continents.created-at
       {:op :column :schema nil :table :continents :name :created-at :as nil}
       '(greatest 1 2)
       {:op :fn-call :name 'greatest :args [{:op :constant :form 1} {:op :constant :form 2}]}
       '(max :continents.created-at)
       {:op :fn-call :name 'max :args [{:op :column :schema nil :table :continents :name :created-at :as nil}]}
       `(max :continents.created-at)
       {:op :fn-call :name `max :args [{:op :column :schema nil :table :continents :name :created-at :as nil}]}
       '(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
       {:op :fn-call :name 'ST_AsText :args [{:op :fn-call :name 'ST_Centroid :args [{:op :constant :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}))
