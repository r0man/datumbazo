(ns datumbazo.test.examples
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        datumbazo.core
        datumbazo.test))

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
  (is (= 0 (drop-continents))))

(database-test test-delete-continents
  (is (= "Delete all rows in the continents database table."
         (:doc (meta #'delete-continents))))
  (is (= 0 (delete-continents)))
  (is (= 0 (count-rows continents-table))))

(database-test test-delete-countries
  (is (= "Delete all rows in the countries database table."
         (:doc (meta #'delete-countries))))
  (is (= 0 (delete-countries)))
  (is (= 0 (count-rows countries-table))))

(database-test test-truncate-continents
  (is (= "Truncate the continents database table."
         (:doc (meta #'truncate-continents))))
  (is (= 0 (truncate-continents :cascade true)))
  (is (= 0 (count-rows continents-table))))

(database-test test-truncate-countries
  (is (= "Truncate the countries database table."
         (:doc (meta #'truncate-countries))))
  (is (= 0 (truncate-countries)))
  (is (= 0 (count-rows countries-table))))

(database-test test-continents
  (is (empty? (continents))))

(database-test test-countries
  (is (empty? (countries))))

(database-test test-continents-by-id
  (is (nil? (continents-by-id 1))))

(database-test test-continents-by-name
  (is (nil? (continents-by-name "Europe"))))
