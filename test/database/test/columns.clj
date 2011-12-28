(ns database.test.columns
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.columns))

(def created-at-column
  (make-column :created-at :timestamp-with-time-zone :default "now()" :not-null true))

(def id-column
  (make-column :id :serial :primary-key true))

(deftest test-column-name
  (are [expected column]
    (is (= (jdbc/as-identifier expected) (column-name column)))
    :created-at created-at-column
    :id id-column))

(deftest test-column-keyword
  (are [expected column]
    (is (= expected (column-keyword column)))
    :created-at created-at-column
    :id id-column))

(deftest test-column-symbol
  (are [expected column]
    (is (= expected (column-symbol column)))
    'created-at created-at-column
    'id id-column))

(deftest test-make-column
  (let [column created-at-column]
    (is (= :created-at (:name column)))
    (is (= :timestamp-with-time-zone (:type column)))
    (is (= "now()" (:default column)))
    (is (:not-null column)))
  (let [column id-column]
    (is (= :id (:name column)))
    (is (= :serial (:type column)))
    (is (nil? (:default column)))
    (is (:not-null column))))
