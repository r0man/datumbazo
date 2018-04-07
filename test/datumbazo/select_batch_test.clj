(ns datumbazo.select-batch-test
  (:require [clojure.test :refer :all]
            [datumbazo.continents :as continents]
            [datumbazo.select-batch :refer :all]
            [datumbazo.test :refer :all]))

(deftest test-select-batch-keyword
  (with-backends [db]
    (let [table :continents
          continents (continents/all db)]
      (is (nil? (select-batch db table [])))
      (is (= (map dissoc-geometry (select-batch db table continents))
             (map dissoc-geometry continents))))))

(deftest test-select-batch-table
  (with-backends [db]
    (let [table (continents/table)
          continents (continents/all db)]
      (is (nil? (select-batch db table [])))
      (is (= (map dissoc-geometry (select-batch db table continents))
             (map dissoc-geometry continents))))))

(deftest test-select-batch-except
  (with-backends [db]
    (let [table (continents/table)
          continents (continents/all db)
          except [:name :geometry]]
      (is (= (select-batch db table continents {:except except})
             (map #(apply dissoc % except) continents))))))

(deftest test-select-batch-by-column
  (with-backends [db]
    (let [rows [{:name "Europe"}
                {:name "Unknown"}
                {:name "Africa"}]]
      (is (= (map :name (select-batch db :continents rows {:join {:name :text}}))
             ["Europe" nil "Africa"])))))
