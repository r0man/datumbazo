(ns datumbazo.test.relation
  (:refer-clojure :exclude [group-by])
  (:use clojure.test
        datumbazo.relation
        datumbazo.sql
        datumbazo.test)
  (:import datumbazo.relation.Relation))

(database-test test-count
  (is (= 1 (count (Relation. (select 1))))))

(deftest test-equiv
  (is (.equiv (Relation. (select 1))
              (Relation. (select 1))))
  (is (not (.equiv (Relation. (select 1))
                   (Relation. (select 2))))))

(database-test test-seq
  (is (= [{:?column? 1}]
         (seq (Relation. (select 1))))))

(deftest test-to-string
  (is (= "[\"SELECT 1\"]"
         (str (Relation. (select 1))))))
