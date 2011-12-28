(ns database.test.registry
  (:use clojure.test
        database.registry
        database.tables))

(deftest test-register-table
  (let [table (make-table :name :photo-thumbnails)]
    (register-table table)
    (is (= table (:photo-thumbnails @*tables*)))))

(deftest test-find-table
  (let [table (make-table :name :photo-thumbnails)]
    (register-table table)
    (is (= table (find-table :photo-thumbnails)))
    (is (= table (find-table 'photo-thumbnails)))
    (is (= table (find-table "photo-thumbnails")))))
