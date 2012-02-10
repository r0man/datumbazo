(ns database.test.registry
  (:use clojure.test
        database.columns
        database.tables
        database.test.examples
        database.registry))

(deftest test-find-table
  (let [table (make-table :photo-thumbnails)]
    (register-table table)
    (is (= table (find-table :photo-thumbnails)))
    (is (= table (find-table 'photo-thumbnails)))
    (is (= table (find-table "photo-thumbnails")))
    (is (= table (find-table (find-table :photo-thumbnails))))))

(deftest test-register-table
  (let [table (make-table :photo-thumbnails)]
    (is (= table (register-table table)))
    (is (= table (:photo-thumbnails @*tables*)))))

(deftest test-with-ensure-table
  (let [table :languages]
    (with-ensure-table table
      (is (table? table))
      (is (= :languages (:name table))))))

(deftest test-with-ensure-column
  (let [table :languages column :id]
    (with-ensure-column table column
      (is (table? table))
      (is (= :languages (:name table)))
      (is (column? column))
      (is (= :id (:name column))))))