(ns database.test.columns
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.core
        database.columns
        database.connection
        database.fixtures))

(def created-at-column
  (make-column :created-at :timestamp-with-time-zone :default "now()" :not-null? true))

(def id-column
  (make-column :id :serial :primary-key? true))

(def iso-639-1-column
  (make-column :iso-639-1 :varchar :length 2 :unique? true :not-null? true))

(deftest test-make-column
  (let [column created-at-column]
    (is (= :created-at (:name column)))
    (is (= :timestamp-with-time-zone (:type column)))
    (is (:native? column))
    (is (nil? (:length column)))
    (is (= "now()" (:default column)))
    (is (:not-null? column)))
  (let [column id-column]
    (is (= :id (:name column)))
    (is (= :serial (:type column)))
    (is (nil? (:length column)))
    (is (:native? column))
    (is (nil? (:default column)))
    (is (:not-null? column)))
  (let [column iso-639-1-column]
    (is (= :iso-639-1 (:name column)))
    (is (= :varchar (:type column)))
    (is (= 2 (:length column)))
    (is (:native? column))
    (is (nil? (:default column)))
    (is (:not-null? column)))
  (let [column (make-column :location [:point-2d])]
    (is (= :location (:name column)))
    (is (= :point-2d (:type column)))
    (is (nil? (:length column)))
    (is (not (:native? column)))
    (is (nil? (:default column)))
    (is (not (:not-null? column)))))

(deftest test-column?
  (is (not (column? nil)))
  (is (not (column? "")))
  (is (column? created-at-column))
  (is (every? column? (vals (:columns (table :languages))))))

(deftest test-column-identifier
  (are [column expected]
    (is (= expected (column-identifier column)))
    :created-at "created_at"
    "created-at" "created_at"))

(deftest test-column-name
  (are [column expected]
    (is (= expected (column-name column)))
    "created-at" "created-at"
    'created-at "created-at"
    :created-at "created-at"
    created-at-column "created-at"))

(deftest test-column-type-name
  (are [expected column]
    (is (= expected (column-type-name column)))
    "timestamp with time zone" created-at-column
    "serial" id-column
    "varchar(2)" iso-639-1-column))

(deftest test-column-spec
  (is (= ["created_at" "timestamp with time zone" "default now()" "not null"]
         (column-spec created-at-column)))
  (is (= ["id" "serial" "primary key" "not null"]
         (column-spec id-column)))
  (is (= ["iso_639_1" "varchar(2)" "not null" "unique"]
         (column-spec iso-639-1-column))))

(deftest test-unique-column?
  (is (not (unique-column? nil)))
  (is (not (unique-column? {})))
  (is (not (unique-column? created-at-column)))
  (is (unique-column? id-column))
  (is (unique-column? iso-639-1-column)))
