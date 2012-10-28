(ns datumbazo.test.meta
  (:import java.sql.DatabaseMetaData)
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        datumbazo.meta
        datumbazo.test))

(database-test test-best-row-identifiers
  (let [columns (best-row-identifiers (jdbc/connection) :table :continents :scope :transaction)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:name %1))
                      (keyword? (:type %1)))
                columns))))

(database-test test-catalogs
  (let [catalogs (catalogs (jdbc/connection))]
    (is (not (empty? catalogs)))
    (is (every? #(keyword (:name %1)) catalogs))))

(database-test test-columns
  (let [columns (columns (jdbc/connection) :table :continents)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:table %1))
                      (keyword? (:name %1))
                      (keyword? (:type %1)))
                columns))))

(database-test test-metadata
  (is (thrown? AssertionError (metadata nil)))
  (is (instance? DatabaseMetaData (metadata (jdbc/connection)))))

(database-test test-indexes
  (let [columns (indexes (jdbc/connection) :table :continents)]
    (is (not (empty? columns)))))

(database-test test-unique-columns
  (let [columns (unique-columns (jdbc/connection) :table :continents)]
    (is (not (empty? columns)))
    (is (= [:id :name :code :freebase-guid :geonames-id] (map :name columns)))))

(database-test test-primary-keys
  (let [columns (primary-keys (jdbc/connection) :table :continents)]
    (is (not (empty? columns)))
    (is (every? #(and (keyword? (:table %1))
                      (keyword? (:schema %1))
                      (keyword? (:name %1)))
                columns))))

(database-test test-schemas
  (let [schemas (schemas (jdbc/connection))]
    (is (not (empty? schemas)))
    (is (every? #(keyword (:name %1)) schemas))))

(database-test test-tables
  (let [tables (tables (jdbc/connection))]
    (is (not (empty? tables)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:name %1))
                      (= :table (:type %1))) tables))))

(database-test test-views
  (let [views (views (jdbc/connection))]
    (is (not (empty? views)))
    (is (every? #(and (keyword? (:schema %1))
                      (keyword? (:name %1))
                      (= :view (:type %1))) views))))
