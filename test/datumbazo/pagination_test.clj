(ns datumbazo.pagination-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.pagination :refer :all]
            [datumbazo.test :refer :all]))

(defn results [db n]
  (sql/select db [:*]
    (sql/from
     (sql/as (sql/values (repeat n {:x 1})) :x [:x]))
    (sql/limit 4)
    (sql/offset 2)))

(deftest test-query
  (with-backends [db]
    (let [sql (results db 10)]
      (is (= (sql/ast (query @sql))
             (sql/ast (query sql))
             (sql/ast sql))))))

(deftest test-page
  (with-backends [db]
    (is (= (page-info db (results db 1))
           {:page 1
            :pages 1
            :per-page 25
            :results 1}))))

(deftest test-next-page
  (with-backends [db]
    (is (= (page-info db (results db (inc *per-page*)))
           {:page 1
            :pages 2
            :per-page 25
            :results 26}))))
