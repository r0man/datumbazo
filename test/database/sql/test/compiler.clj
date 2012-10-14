(ns database.sql.test.compiler
  (:use clojure.test
        database.sql.compiler))

(deftest test-compile-sql
  (are [arg expected]
       (is (= expected (compile-sql arg)))
       nil ["NULL"]
       "continents" ["continents"]
       "public.continents" ["public.continents"]
       :continents ["continents"]
       :public.continents ["public.continents"]))

(deftest test-to-table
  (are [arg expected]
       (is (= expected (to-table arg)))
       "continents"
       (map->Table {:name :continents})
       "public.continents"
       (map->Table {:schema :public :name :continents})
       :continents
       (map->Table {:name :continents})
       :public.continents
       (map->Table {:schema :public :name :continents})
       (map->Table {:schema :public :name :continents})
       (map->Table {:schema :public :name :continents})))
