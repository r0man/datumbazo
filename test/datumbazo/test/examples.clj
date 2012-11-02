(ns datumbazo.test.examples
  (:refer-clojure :exclude [group-by])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        datumbazo.core
        datumbazo.test))

(def africa
  {:name "Africa" :code "af"})

(def europe
  {:name "Europe" :code "eu"})

(deftable continents
  "The continents database table."
  (column :id :serial)
  (column :name :text))

(deftable countries
  "The countries database table."
  (column :id :serial)
  (column :continent-id :integer :references :continents/id)
  (column :name :text))

(deftest test-continents-table
  (is (= :continents (:name continents-table)))
  (is (= [:id :name] (:columns continents-table)))
  (let [column (:id (:column continents-table))]
    (is (= :id (:name column)))
    (is (= :serial (:type column))))
  (let [column (:name (:column continents-table))]
    (is (= :name (:name column)))
    (is (= :text (:type column)))))

(deftest test-countries-table
  (is (= :countries (:name countries-table)))
  (is (= [:id :continent-id :name] (:columns countries-table)))
  (let [column (:id (:column countries-table))]
    (is (= :id (:name column)))
    (is (= :serial (:type column))))
  (let [column (:continent-id (:column countries-table))]
    (is (= :continent-id (:name column)))
    (is (= :integer (:type column))))
  (let [column (:name (:column countries-table))]
    (is (= :name (:name column)))
    (is (= :text (:type column)))))

(database-test test-drop-continents
  (is (= "Drop the continents database table."
         (:doc (meta #'drop-continents))))
  (drop-countries)
  (is (= 0 (drop-continents)))
  (is (= 0 (drop-continents :if-exists true))))

(database-test test-delete-continents
  (is (= "Delete all rows in the continents database table."
         (:doc (meta #'delete-continents))))
  (is (= 0 (delete-continents)))
  (is (= 0 (count-all :continents))))

(database-test test-delete-countries
  (is (= "Delete all rows in the countries database table."
         (:doc (meta #'delete-countries))))
  (is (= 0 (delete-countries)))
  (is (= 0 (count-all :countries))))

(database-test test-insert-continent
  (let [row (insert-continent europe)]
    (is (number? (:id row)))
    (is (= "Europe" (:name row)))
    (is (= "eu" (:code row)))
    (is (thrown? Exception (insert-continent row)))))

(database-test test-insert-continents
  (let [rows (insert-continents [africa europe])]
    (let [row (first rows)]
      (is (number? (:id row)))
      (is (= "Africa" (:name row)))
      (is (= "af" (:code row))))
    (let [row (second rows)]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "eu" (:code row))))))

(database-test test-save-continent
  (let [row (save-continent europe)]
    (is (number? (:id row)))
    (is (= "Europe" (:name row)))
    (is (= "eu" (:code row)))
    (is (= row (save-continent row)))))

(database-test test-truncate-continents
  (is (= "Truncate the continents database table."
         (:doc (meta #'truncate-continents))))
  (is (= 0 (truncate-continents :cascade true)))
  (is (= 0 (truncate-continents :cascade true :if-exists true)))
  (is (= 0 (count-all :continents))))

(database-test test-truncate-countries
  (is (= "Truncate the countries database table."
         (:doc (meta #'truncate-countries))))
  (is (= 0 (truncate-countries)))
  (is (= 0 (count-all :countries))))

(database-test test-update-continent
  (is (nil? (update-continent europe)))
  (let [europe (insert-continent europe)
        row (update-continent (assoc europe :name "Europa"))]
    (is (number? (:id row)))
    (is (= "Europa" (:name row)))
    (is (= "eu" (:code row)))
    (let [row (update-continent (assoc row :name "Europe"))]
      (is (number? (:id row)))
      (is (= "Europe" (:name row)))
      (is (= "eu" (:code row))))))

(database-test test-continents
  (is (empty? (continents))))

(database-test test-countries
  (is (empty? (countries))))

(database-test test-continents-by-id
  (is (empty? (continents-by-id 1)))
  (is (empty? (continents-by-id "1"))))

(database-test test-continents-by-name
  (is (empty? (continents-by-name "Europe"))))
