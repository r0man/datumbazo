(ns database.test.tables
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.tables
        database.registry))

(deftable photo-thumbnails
  [[:id :serial :primary-key true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null true :default "now()"]])

(def photo-thumbnails-table (find-table :photo-thumbnails))

(deftest test-table-name
  (are [expected-keyword table]
    (is (= (jdbc/as-identifier expected-keyword) (table-name table)))
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-keyword
  (are [expected table]
    (is (= expected (table-keyword table)))
    :photo-thumbnails photo-thumbnails-table))

(deftest test-table-symbol
  (are [expected table]
    (is (= expected (table-symbol table)))
    'photo-thumbnails photo-thumbnails-table))

(deftest test-make-table
  (let [columns (:columns photo-thumbnails-table)
        table (make-table :name :photo-thumbnails :columns columns)]
    (is (= :photo-thumbnails (:name table)))
    (is (= columns (:columns table)))))
