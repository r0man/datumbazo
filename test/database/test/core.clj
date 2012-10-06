(ns database.test.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.core
        database.test
        clojure.test))

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
  (is (= :continents (:name continents)))
  (is (= [:id :name] (:columns continents)))
  (let [column (:id (:column continents))]
    (is (= :id (:name column)))
    (is (= :serial (:type column))))
  (let [column (:name (:column continents))]
    (is (= :name (:name column)))
    (is (= :text (:type column)))))

(deftest test-countries-table
  (is (= :countries (:name countries)))
  (is (= [:id :continent-id :name] (:columns countries)))
  (let [column (:id (:column countries))]
    (is (= :id (:name column)))
    (is (= :serial (:type column))))
  (let [column (:continent-id (:column countries))]
    (is (= :continent-id (:name column)))
    (is (= :integer (:type column))))
  (let [column (:name (:column countries))]
    (is (= :name (:name column)))
    (is (= :text (:type column)))))

(database-test test-count-rows
  (is (= 0 (count-rows :continents))))

(database-test test-delete-table
  (is (= 0 (delete-table :continents)))
  (is (= 0 (count-rows :continents))))

(database-test test-delete-continents
  (is (= "Delete all rows in the database table :continents."
         (:doc (meta #'delete-continents))))
  (is (= 0 (delete-continents)))
  (is (= 0 (count-rows :continents))))

(database-test test-delete-countries
  (is (= "Delete all rows in the database table :countries."
         (:doc (meta #'delete-countries))))
  (is (= 0 (delete-countries)))
  (is (= 0 (count-rows :countries))))

(database-test test-truncate-table
  (is (= 0 (truncate-table :continents :cascade true)))
  (is (= 0 (count-rows :continents)))
  (is (= 0 (truncate-table :countries)))
  (is (= 0 (count-rows :countries))))

(database-test test-truncate-continents
  (is (= "Truncate the database table :continents."
         (:doc (meta #'truncate-continents))))
  (is (= 0 (truncate-continents :cascade true)))
  (is (= 0 (count-rows :continents))))

(database-test test-truncate-countries
  (is (= "Truncate the database table :countries."
         (:doc (meta #'truncate-countries))))
  (is (= 0 (truncate-countries)))
  (is (= 0 (count-rows :countries))))
