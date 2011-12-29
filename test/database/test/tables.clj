(ns database.test.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.tables
        database.registry
        database.test.examples))

(def photo-thumbnails-table (find-table :photo-thumbnails))

(deftest test-make-table
  (let [columns (:columns photo-thumbnails-table)
        table (make-table :name :photo-thumbnails :columns columns)]
    (is (= :photo-thumbnails (:name table)))
    (is (= columns (:columns table)))))

(deftest test-table-identifier
  (are [expected-keyword table]
    (is (= (jdbc/as-identifier expected-keyword) (table-identifier table)))
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-keyword
  (are [expected table]
    (is (= expected (table-keyword table)))
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-symbol
  (are [expected table]
    (is (= expected (table-symbol table)))
    'photo-thumbnails photo-thumbnails-table))
