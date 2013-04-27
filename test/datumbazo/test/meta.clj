(ns datumbazo.test.meta
  (:import java.sql.DatabaseMetaData)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [subset?]])
  (:use clojure.test
        datumbazo.meta
        datumbazo.test))

(database-test test-best-row-identifiers
  (let [columns (best-row-identifiers db :table :continents :scope :transaction)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:name %1))
                      (keyword? (:type %1)))
                columns))))

(database-test test-catalogs
  (let [catalogs (catalogs db)]
    (is (not (empty? catalogs)))
    (is (every? #(keyword (:name %1)) catalogs))))

(database-test test-columns
  (let [columns (columns db :table :continents)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:table %1))
                      (keyword? (:name %1))
                      (keyword? (:type %1)))
                columns))
    (is (= [:id :name :code :geometry :freebase-guid :geonames-id :created-at :updated-at]
           (map :name columns))))
  (let [columns (columns db :table :countries :name :continent-id)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:table %1))
                      (keyword? (:name %1))
                      (keyword? (:type %1)))
                columns))
    (is (= [:continent-id] (map :name columns)))))

(database-test test-indexes
  (let [columns (indexes db :table :continents)]
    (is (not (empty? columns)))))

(database-test test-unique-columns
  (let [columns (unique-columns db :table :continents)]
    (is (not (empty? columns)))
    (is (= [:id :name :code :freebase-guid :geonames-id] (map :name columns)))))

(database-test test-primary-keys
  (let [columns (primary-keys db :table :continents)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:table %1))
                      (keyword? (:schema %1))
                      (keyword? (:name %1)))
                columns))
    (is (= [:id] (map :name columns)))))

(database-test test-schemas
  (let [schemas (schemas db)]
    (is (not (empty? schemas)))
    (is (every? #(keyword (:name %1)) schemas))
    (is (is (subset? (set [:information-schema :pg-catalog :public :twitter])
                     (set (map :name schemas)))))))

(database-test test-tables
  (let [tables (tables db)]
    (is (not (empty? tables)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:name %1))
                      (= :table (:type %1))) tables))
    (is (subset? (set (map :name tables))
                 (set [:continents :countries :spatial-ref-sys :changes :dependencies :events :projects :tags :tweets :users])))))

(database-test test-views
  (let [views (views db)]
    (is (not (empty? views)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:name %1))
                      (= :view (:type %1))) views))))
