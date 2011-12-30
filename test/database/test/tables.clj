(ns database.test.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.columns
        database.tables
        database.test.examples))

(deftest test-find-table
  (let [table (make-table :photo-thumbnails)]
    (register-table table)
    (is (= table (find-table :photo-thumbnails)))
    (is (= table (find-table 'photo-thumbnails)))
    (is (= table (find-table "photo-thumbnails")))
    (is (= table (find-table (find-table :photo-thumbnails))))))

(deftest test-make-table
  (let [table (make-table :test [[:id :serial]])]
    (is (= :test (:name table)))
    (let [columns (:columns table)]
      (is (= 1 (count columns)))
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

(deftest test-table-identifier
  (are [expected-keyword table]
    (is (= (jdbc/as-identifier expected-keyword) (table-identifier table)))
    :photo-thumbnails :photo-thumbnails
    :photo-thumbnails 'photo-thumbnails
    "photo-thumbnails" "photo-thumbnails"
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-keyword
  (are [expected table]
    (is (= expected (table-keyword table)))
    :photo-thumbnails :photo-thumbnails
    :photo-thumbnails 'photo-thumbnails
    :photo-thumbnails "photo-thumbnails"
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-symbol
  (are [expected table]
    (is (= expected (table-symbol table)))
    'photo-thumbnails :photo-thumbnails
    'photo-thumbnails 'photo-thumbnails
    'photo-thumbnails "photo-thumbnails"
    'photo-thumbnails photo-thumbnails-table))

(deftest test-register-table
  (let [table (make-table :photo-thumbnails)]
    (register-table table)
    (is (= table (:photo-thumbnails @*tables*)))))
