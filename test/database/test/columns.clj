(ns database.test.columns
  (:use clojure.test
        database.columns))

(def example-column (make-column :name "created_at"))

(deftest test-column-name
  (is (= "created_at" (column-name example-column))))

(deftest test-column-keyword
  (is (= :created-at (column-keyword example-column))))

(deftest test-column-symbol
  (is (= 'created-at (column-symbol example-column))))

(deftest test-make-column
  (let [column example-column]
    (is (= "created_at" (:name column)))))
