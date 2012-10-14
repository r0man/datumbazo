(ns database.sql.test.compiler
  (:use clojure.test
        database.sql.compiler))

(deftest test-column?
  (is (not (column? nil)))
  (is (column? (map->Column {}))))

(deftest test-compile-sql
  (are [arg expected]
       (is (= expected (compile-sql arg)))
       nil ["NULL"]
       "continents" ["continents"]
       "public.continents" ["public.continents"]
       :continents ["continents"]
       :public.continents ["public.continents"]
       (->Table nil :continents) ["continents"]
       (->Table :public :continents) ["public.continents"]))

(deftest test-table?
  (is (not (table? nil)))
  (is (table? (map->Table {}))))

(deftest test-to-column
  (are [arg expected]
       (is (= expected (to-column arg)))
       "id"
       (map->Column {:name :id})
       "continents.id"
       (map->Column {:table :continents :name :id})
       :continents.id
       (map->Column {:table :continents :name :id})
       :public.continents.id
       (map->Column {:schema :public :table :continents :name :id})
       :public.continents/id
       (map->Column {:schema :public :table :continents :name :id})
       (map->Column {:schema :public :table :continents :name :id})
       (map->Column {:schema :public :table :continents :name :id})
       (to-column [:continents.id :as :continent-id])
       (map->Column {:table :continents :name :id :alias :continent-id})))

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
       (map->Table {:schema :public :name :continents})
       (to-table [:continents :as :c])
       (map->Table {:name :continents :alias :c})))
