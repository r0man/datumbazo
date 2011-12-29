(ns database.test.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.tables
        database.test.examples))

(deftest test-find-table
  (let [table (make-table :name :photo-thumbnails)]
    (register-table table)
    (is (= table (find-table :photo-thumbnails)))
    (is (= table (find-table 'photo-thumbnails)))
    (is (= table (find-table "photo-thumbnails")))))

(deftest test-make-table
  (let [columns (:columns photo-thumbnails-table)
        table (make-table :name :photo-thumbnails :columns columns)]
    (is (= :photo-thumbnails (:name table)))
    (is (= columns (:columns table)))))

(deftest test-table?
  (is (not (table? nil)))
  (is (not (table? "")))
  (is (table? (make-table :name :continents))))

(deftest test-table-name
  (are [expected table]
    (is (= expected (table-name table)))
    :photo-thumbnails :photo-thumbnails
    :photo-thumbnails (find-table :photo-thumbnails)))

(deftest test-table-identifier
  (are [expected-keyword table]
    (is (= (jdbc/as-identifier expected-keyword) (table-identifier table)))
    :photo-thumbnails photo-thumbnails-table
    :photo-thumbnails :photo-thumbnails
    :photo-thumbnails 'photo-thumbnails
    "photo-thumbnails" "photo-thumbnails"))

(deftest test-table-keyword
  (are [expected table]
    (is (= expected (table-keyword table)))
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-symbol
  (are [expected table]
    (is (= expected (table-symbol table)))
    'photo-thumbnails photo-thumbnails-table))

(deftest test-register-table
  (let [table (make-table :name :photo-thumbnails)]
    (register-table table)
    (is (= table (:photo-thumbnails @*tables*)))))
