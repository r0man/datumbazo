(ns datumbazo.test.core
  (:refer-clojure :exclude [group-by])
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.core
        datumbazo.test
        datumbazo.test.examples
        clojure.test))

(database-test test-count-all
  (is (= 0 (count-all :continents))))

(deftest test-make-table
  (let [table (make-table :continents)]
    (is (= :continents (:name table))))
  (is (= (make-table :continents)
         (make-table "continents"))))

(database-test test-insert-record
  (let [rows (run (insert :continents {:name "Europe" :code "eu"}))]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "eu" (:code row)))))
  (let [rows (run (insert :continents [{:name "North America" :code "na"} {:name "South America" :code "sa"}]))]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "North America" (:name row)))
      (is (= "na" (:code row))))
    (let [row (second rows)]
      (is (number? (:id row)))
      (is (= "South America" (:name row)))
      (is (= "sa" (:code row))))))

(database-test test-update-record
  (let [europe (first (run (insert :continents {:name "Europe" :code "eu"})))
        rows (run (update :continents (assoc europe :name "Europa")))]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "Europa" (:name row)))
      (is (= "eu" (:code row))))))
