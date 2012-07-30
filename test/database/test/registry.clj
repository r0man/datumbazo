(ns database.test.registry
  (:use clojure.test
        database.columns
        database.tables
        database.fixtures
        database.registry))

(deftest test-find-table
  (let [table (make-table :photo-thumbnails)]
    (register-table table)
    (is (table? (find-table :photo-thumbnails)))
    (is (= table (find-table :photo-thumbnails)))
    (is (= table (find-table 'photo-thumbnails)))
    (is (= table (find-table "photo-thumbnails")))
    (is (= table (find-table (find-table :photo-thumbnails))))))

(deftest test-register-table
  (let [table (make-table :photo-thumbnails)]
    (is (= table (register-table table)))
    (is (= table (:photo-thumbnails @*tables*)))))

(deftest test-with-ensure-table
  (with-ensure-table [languages :languages]
    (is (table? languages))
    (is (= "languages" (:name languages))))
  (with-ensure-table [languages (find-table :languages)]
    (is (table? languages))
    (is (= "languages" (:name languages)))))

(deftest test-with-ensure-column
  (with-ensure-column [:languages [column :id]]
    (is (column? column))
    (is (= :id (:name column)))
    (is (= (find-table :languages) (:table column))))
  (with-ensure-column [(find-table :languages) [column :id]]
    (is (column? column))
    (is (= :id (:name column)))
    (is (= (find-table :languages) (:table column)))))
