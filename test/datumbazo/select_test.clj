(ns datumbazo.select-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]))

(deftest test-select-array
  (with-backends [db]
    (is (= [{:array [1 2]}]
           @(sql/select db [[1 2]])))
    (is (= @(sql/select db [[[1 2]]])
           [{:array [[1 2]]}]))
    (is (= @(sql/select db [[["1" "2"]]])
           [{:array [["1" "2"]]}]))
    (is (= @(sql/select db [[[[1 2]]]])
           [{:array [[[1 2]]]}]))))

(deftest test-select-array-concat
  (with-backends [db]
    (is (= [{:?column? [1 2 3 4 5 6]}]
           @(sql/select db ['(|| [1 2] [3 4] [5 6])])))))

;; PostgreSQL JSON Support Functions

(deftest test-select-array-to-json
  (with-backends [db]
    (is (= [{:array_to_json [[1 5] [99 100]]}]
           @(sql/select db [`(array_to_json (cast "{{1,5},{99,100}}" ~(keyword "int[]")))])))))

(deftest test-select-row-to-json
  (with-backends [db]
    (is (= [{:row_to_json {:f1 1, :f2 "foo"}}]
           @(sql/select db ['(row_to_json (row 1 "foo"))])))))

(deftest test-select-to-json
  (with-backends [db]
    (is (= [{:to_json "Fred said \"Hi.\""}]
           @(sql/select db ['(to_json "Fred said \"Hi.\"")])))))

(deftest test-cast-string-to-int
  (with-test-dbs [db]
    (when-not (= :mysql (:name db))
      (is (= @(sql/select db [`(cast "1" :int)])
             [(case (:scheme db)
                :mysql {(keyword "cast('1' as int)") 1}
                :postgresql {:int4 1}
                :sqlite {(keyword "cast(? as int)") 1})])))))

(deftest test-select-1
  (with-test-dbs [db]
    (is (= @(sql/select db [1])
           [(case (:scheme db)
              :postgresql {:?column? 1}
              {:1 1})]))))

(deftest test-select-1-2-3
  (with-test-dbs [db]
    (is (= @(sql/select db [1 2 3])
           [(case (:scheme db)
              :postgresql {:?column? 1 :?column?_2 2 :?column?_3 3}
              {:1 1 :2 2 :3 3})]))))

(deftest test-select-1-as-n
  (with-test-dbs [db]
    (is (= @(sql/select db [(sql/as 1 :n)])
           [{:n 1}]))))

(deftest test-select-x-as-n
  (with-test-dbs [db]
    (is (= @(sql/select db [(sql/as "x" :n)])
           [{:n "x"}]))))

(deftest test-test-select-1-2-3-as
  (with-test-dbs [db]
    (is (= @(sql/select db [(sql/as 1 :a) (sql/as 2 :b) (sql/as 3 :c)])
           [{:a 1, :b 2, :c 3}]))))

(deftest test-select-1-in-list
  (with-test-dbs [db #{:postgresql :sqlite}]
    (is (= [{:a 1}] @(sql/select db [(sql/as 1 :a)]
                       (sql/where `(in 1 ~(list 1 2 3))))))))

(deftest test-select-concat-strings
  (with-test-dbs [db]
    (is (= [(case (:scheme db)
              :mysql {(keyword "('a' || 'b' || 'c')") 0} ;; not string concat, but OR operator
              :postgresql {:?column? "abc"}
              :sqlite {(keyword "(? || ? || ?)") "abc"})]
           @(sql/select db ['(|| "a" "b" "c")])))))

(deftest test-select-alias
  (with-backends [db]
    (is (= @(sql/select db [(sql/as 1 :x)])
           [{:x 1}]))))

(deftest test-select-subselect
  (with-backends [db]
    (is (= @(sql/select db [:*]
              (sql/from (sql/as (sql/select db [(sql/as 1 :x) (sql/as 2 :y)]) :z)))
           [{:x 1 :y 2}]))))

(deftest test-select-order-by
  (with-backends [db]
    (is (= @(sql/select db [:name]
              (sql/from :continents)
              (sql/order-by :name))
           [{:name "Africa"}
            {:name "Antarctica"}
            {:name "Asia"}
            {:name "Europe"}
            {:name "North America"}
            {:name "Oceania"}
            {:name "South America"}]))))

(deftest test-select-1-as-a-2-as-b-3-as-c
  (with-backends [db]
    (is (= [{:a 1 :b 2 :c 3}]
           @(sql/select db [(sql/as 1 :a)
                            (sql/as 2 :b)
                            (sql/as 3 :c)])))))

(deftest test-select-cast-int-as-text
  (with-backends [db]
    (is (= [{:text "1"}]
           @(sql/select db [(sql/as `(cast 1 :text) :text)])))))

(deftest test-select-deref
  (with-backends [db]
    (is (= @(sql/select db [(sql/as 1 :a)
                            (sql/as 2 :b)
                            (sql/as 3 :c)])
           [{:a 1 :b 2 :c 3}]))))

(deftest test-select-bigint
  (with-backends [db]
    (is (= @(sql/select db [(bigint 1)])
           [{:?column? 1}]))))

(deftest test-select-qualified
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/select db [(sql/as :did :distributor/id)
                              (sql/as :dname :distributor/name)]
                (sql/from :distributors))
             [{:distributor/id 5 :distributor/name "Gizmo Transglobal"}
              {:distributor/id 6 :distributor/name "Associated Computing, Inc"}
              {:distributor/id 7 :distributor/name "Redline GmbH"}
              {:distributor/id 8 :distributor/name "Anvil Distribution"}
              {:distributor/id 9 :distributor/name "Antwerp Design"}
              {:distributor/id 10 :distributor/name "Conrad International"}])))))

;; Window functions: http://www.postgresql.org/docs/9.4/static/tutorial-window.html

(deftest test-compare-salaries
  (with-backends [db]
    (with-test-table db :empsalary
      (is (= @(sql/select db [:depname :empno :salary '(over (avg :salary) (partition-by :depname))]
                (sql/from :empsalary))
             [{:avg 5020.0000000000000000M
               :salary 5200
               :empno 11
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 4200
               :empno 7
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 4500
               :empno 9
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 6000
               :empno 8
               :depname "develop"}
              {:avg 5020.0000000000000000M
               :salary 5200
               :empno 10
               :depname "develop"}
              {:avg 3700.0000000000000000M
               :salary 3500
               :empno 5
               :depname "personnel"}
              {:avg 3700.0000000000000000M
               :salary 3900
               :empno 2
               :depname "personnel"}
              {:avg 4866.6666666666666667M
               :salary 4800
               :empno 3
               :depname "sales"}
              {:avg 4866.6666666666666667M
               :salary 5000
               :empno 1
               :depname "sales"}
              {:avg 4866.6666666666666667M
               :salary 4800
               :empno 4
               :depname "sales"}])))))

(deftest test-rank-over-order-by
  (with-backends [db]
    (with-test-table db :empsalary
      (is (= @(sql/select db [:depname :empno :salary '(over (rank) (partition-by :depname (order-by (desc :salary))))]
                (sql/from :empsalary))
             [{:rank 1 :salary 6000 :empno 8 :depname "develop"}
              {:rank 2 :salary 5200 :empno 10 :depname "develop"}
              {:rank 2 :salary 5200 :empno 11 :depname "develop"}
              {:rank 4 :salary 4500 :empno 9 :depname "develop"}
              {:rank 5 :salary 4200 :empno 7 :depname "develop"}
              {:rank 1 :salary 3900 :empno 2 :depname "personnel"}
              {:rank 2 :salary 3500 :empno 5 :depname "personnel"}
              {:rank 1 :salary 5000 :empno 1 :depname "sales"}
              {:rank 2 :salary 4800 :empno 4 :depname "sales"}
              {:rank 2 :salary 4800 :empno 3 :depname "sales"}])))))

(deftest test-window-over-empty
  (with-backends [db]
    (with-test-table db :empsalary
      (is (= @(sql/select db [:salary '(over (sum :salary))]
                (sql/from :empsalary))
             [{:sum 47100 :salary 5200}
              {:sum 47100 :salary 5000}
              {:sum 47100 :salary 3500}
              {:sum 47100 :salary 4800}
              {:sum 47100 :salary 3900}
              {:sum 47100 :salary 4200}
              {:sum 47100 :salary 4500}
              {:sum 47100 :salary 4800}
              {:sum 47100 :salary 6000}
              {:sum 47100 :salary 5200}])))))

(deftest test-window-sum-over-order-by
  (with-backends [db]
    (with-test-table db :empsalary
      (is (= @(sql/select db [:salary '(over (sum :salary) (order-by :salary))]
                (sql/from :empsalary))
             [{:sum 3500 :salary 3500}
              {:sum 7400 :salary 3900}
              {:sum 11600 :salary 4200}
              {:sum 16100 :salary 4500}
              {:sum 25700 :salary 4800}
              {:sum 25700 :salary 4800}
              {:sum 30700 :salary 5000}
              {:sum 41100 :salary 5200}
              {:sum 41100 :salary 5200}
              {:sum 47100 :salary 6000}])))))

(deftest test-window-rank-over-partition-by
  (with-backends [db]
    (with-test-table db :empsalary
      (is (= @(sql/select db [:depname :empno :salary :enroll-date]
                (sql/from (sql/as (sql/select db [:depname :empno :salary :enroll-date
                                                  (sql/as '(over (rank) (partition-by :depname (order-by (desc :salary) :empno))) :pos)]
                                    (sql/from :empsalary))
                                  :ss))
                (sql/where '(< pos 3)))
             [{:depname "develop"
               :empno 8
               :salary 6000
               :enroll-date #inst "2006-10-01T00:00:00.000-00:00"}
              {:depname "develop"
               :empno 10
               :salary 5200
               :enroll-date #inst "2007-08-01T00:00:00.000-00:00"}
              {:depname "personnel"
               :empno 2
               :salary 3900
               :enroll-date #inst "2006-12-23T00:00:00.000-00:00"}
              {:depname "personnel"
               :empno 5
               :salary 3500
               :enroll-date #inst "2007-12-10T00:00:00.000-00:00"}
              {:depname "sales"
               :empno 1
               :salary 5000
               :enroll-date #inst "2006-10-01T00:00:00.000-00:00"}
              {:depname "sales"
               :empno 3
               :salary 4800
               :enroll-date #inst "2007-08-01T00:00:00.000-00:00"}])))))

(deftest test-window-alias
  (with-backends [db]
    (with-test-table db :empsalary
      (is (= @(sql/select db ['(over (sum :salary) :w)
                              '(over (avg :salary) :w)]
                (sql/from :empsalary)
                (sql/window (sql/as '(partition-by :depname (order-by (desc salary))) :w)))
             [{:avg 6000.0000000000000000M :sum 6000}
              {:avg 5466.6666666666666667M :sum 16400}
              {:avg 5466.6666666666666667M :sum 16400}
              {:avg 5225.0000000000000000M :sum 20900}
              {:avg 5020.0000000000000000M :sum 25100}
              {:avg 3900.0000000000000000M :sum 3900}
              {:avg 3700.0000000000000000M :sum 7400}
              {:avg 5000.0000000000000000M :sum 5000}
              {:avg 4866.6666666666666667M :sum 14600}
              {:avg 4866.6666666666666667M :sum 14600}])))))

(deftest test-cast-array
  (is (= @(sql/select db ['(cast [] [:uuid])])
         [{:array []}]))
  (is (= @(sql/select db ['(cast ["f9fd3df1-59f4-454c-90d3-c62a43fb035f"] [:uuid])])
         [{:array [#uuid "f9fd3df1-59f4-454c-90d3-c62a43fb035f"]}])))

(deftest test-array-subvec
  (is (= @(sql/select db [`(array_subvec [1 2 3 4] 1 2)])
         [{:array [1 2]}])))

(deftest test-array-agg-order-by-asc
  (with-backends [db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (is (= @(sql/select db [`(array_agg :name (order-by (asc :id)))]
              (sql/from :books)
              (sql/group-by :author-id))
           [{:array_agg ["Book #1" "Book #4"]}
            {:array_agg ["Book #2" "Book #5"]}
            {:array_agg ["Book #3" "Book #6" "Book #8"]}
            {:array_agg ["Book #7"]}]))))

(deftest test-array-agg-order-by-desc
  (with-backends [db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (is (= @(sql/select db [`(array_agg :name (order-by (desc :id)))]
              (sql/from :books)
              (sql/group-by :author-id))
           [{:array_agg ["Book #4" "Book #1"]}
            {:array_agg ["Book #5" "Book #2"]}
            {:array_agg ["Book #8" "Book #6" "Book #3"]}
            {:array_agg ["Book #7"]}]))))

(deftest test-array-agg-order-by-many-columns
  (with-backends [db]
    (setup-test-table db :authors)
    (setup-test-table db :books)
    (is (= @(sql/select db [`(array_agg :name (order-by :name :id))]
              (sql/from :books)
              (sql/group-by :author-id))
           [{:array_agg ["Book #1" "Book #4"]}
            {:array_agg ["Book #2" "Book #5"]}
            {:array_agg ["Book #3" "Book #6" "Book #8"]}
            {:array_agg ["Book #7"]}]))))
