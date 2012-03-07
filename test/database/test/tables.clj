(ns database.test.tables
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
  (are [table expected]
    (is (= expected (table-name table)))
    "photo-thumbnails" "photo-thumbnails"
    'photo-thumbnails "photo-thumbnails"
    :photo-thumbnails "photo-thumbnails"
    photo-thumbnails "photo-thumbnails"
    (find-table :photo-thumbnails) "photo-thumbnails"))

(deftest test-table-identifier
  (are [table expected]
    (is (= expected (table-identifier table)))
    "photo-thumbnails" "photo_thumbnails"
    'photo-thumbnails "photo_thumbnails"
    :photo-thumbnails "photo_thumbnails"
    photo-thumbnails "photo_thumbnails"
    (find-table :photo-thumbnails) "photo_thumbnails"))
