(ns datumbazo.test.core
  (:refer-clojure :exclude [group-by])
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.core
        datumbazo.test
        datumbazo.test.examples
        clojure.test))

(deftest test-as-identifier
  (are [obj expected]
       (is (= expected (as-identifier obj)))
       :a-1 "a-1"
       "a-1" "a-1"
       "a_1" "a_1"
       {:schema :public :table :continents}
       "public.continents"
       {:schema :public :table :continents :name :id}
       "public.continents.id"))

(deftest test-as-keyword
  (are [obj expected]
       (is (= expected (as-keyword obj)))
       :a-1 :a-1
       "a-1" :a-1
       "a_1" :a-1
       {:schema :public :table :continents}
       :public.continents
       {:schema :public :table :continents :name :id}
       :public.continents.id))

(database-test test-count-all
  (is (= 0 (count-all :continents))))

(deftest test-make-table
  (let [table (make-table :continents)]
    (is (= :continents (:name table))))
  (is (= (make-table :continents)
         (make-table "continents"))))

(database-test test-insert
  (let [rows (run (insert :continents [{:name "North America" :code "na"} {:name "South America" :code "sa"}]))]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "North America" (:name row)))
      (is (= "na" (:code row))))
    (let [row (second rows)]
      (is (number? (:id row)))
      (is (= "South America" (:name row)))
      (is (= "sa" (:code row))))))

(database-test test-update
  (let [europe (first (run (insert :continents [{:name "Europe" :code "eu"}])))
        rows (run (update :continents (assoc europe :name "Europa")))]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "Europa" (:name row)))
      (is (= "eu" (:code row))))))
