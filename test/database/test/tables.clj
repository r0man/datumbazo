(ns database.test.tables
  (:use clojure.test
        database.tables))

(def example-table (make-table :name "photo_thumbnails"))

(deftest test-table-name
  (is (= "photo_thumbnails" (table-name example-table))))

(deftest test-table-keyword
  (is (= :photo-thumbnails (table-keyword example-table))))

(deftest test-table-symbol
  (is (= 'photo-thumbnails (table-symbol example-table))))

(deftest test-make-table
  (let [table example-table]
    (is (= "photo_thumbnails" (:name table)))))
