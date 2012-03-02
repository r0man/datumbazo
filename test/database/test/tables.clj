(ns database.test.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.columns
        database.tables
        database.test.examples
        database.registry))

(deftest test-make-table
  (let [table (make-table :test [[:id :serial] [:name :text]] :url identity)]
    (is (= :test (:name table)))
    (is (= #{:id :name} (set (keys (:columns table)))))
    (is (= identity (:url table)))
    (let [columns (vals (:columns table))]
      (is (= 2 (count columns)))
      (is (every? column? columns)))))

(deftest test-table?
  (is (not (table? nil)))
  (is (not (table? "")))
  (is (table? (make-table :continents))))

(deftest test-table-name
  (are [expected table]
    (is (= expected (table-name table)))
    :photo-thumbnails :photo-thumbnails
    :photo-thumbnails (find-table :photo-thumbnails)))

;; (table-identifier (find-table :continents))

;; (table? (find-table :continents))

;; (table? (find-table :continents))
;; (instance? database.tables.Table (find-table :continents))

;; (= database.tables.Table (class (find-table :continents)))

(deftest test-table-identifier
  (are [table expected]
    (is (= expected (table-identifier table)))
    :photo-thumbnails "photo_thumbnails"
    "photo-thumbnails" "photo_thumbnails"))

(deftest test-table-keyword
  (are [expected table]
    (is (= expected (table-keyword table)))
    :photo-thumbnails :photo-thumbnails
    :photo-thumbnails 'photo-thumbnails
    :photo-thumbnails "photo-thumbnails"
    :photo-thumbnails photo-thumbnails))

(deftest test-table-symbol
  (are [expected table]
    (is (= expected (table-symbol table)))
    'photo-thumbnails :photo-thumbnails
    'photo-thumbnails 'photo-thumbnails
    'photo-thumbnails "photo-thumbnails"
    'photo-thumbnails photo-thumbnails))
