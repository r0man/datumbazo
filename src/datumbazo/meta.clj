(ns datumbazo.meta
  (:import java.sql.DatabaseMetaData)
  (:refer-clojure :exclude [resultset-seq])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [lower-case]]
            [sqlingvo.core :refer [sql-name]]
            [datumbazo.connection :refer [with-connection]]
            [inflections.core :refer [hyphenize hyphenize-keys underscore]]))

(defn- hyphenize-keyword [k]
  (if k (keyword (hyphenize (name k)))))

(defn- resultset-seq [^java.sql.ResultSet rs]
  (->> (clojure.core/resultset-seq rs)
       (map #(hyphenize-keys (merge {} %1)))))

(defn best-row-identifiers
  "Retrieves a description of a table's optimal set of columns that uniquely identifies a row."
  [db & {:keys [catalog schema table scope nullable entities]}]
  (with-connection [db db]
    (->> (.getBestRowIdentifier
          (.getMetaData (:connection db))
          (if catalog (sql-name db catalog))
          (if schema (sql-name db schema))
          (if table (sql-name db table))
          (condp = scope
            :temporary DatabaseMetaData/bestRowTemporary
            :transaction DatabaseMetaData/bestRowTransaction
            :session DatabaseMetaData/bestRowSession
            DatabaseMetaData/bestRowTemporary)
          (not (nil? nullable)))
         (resultset-seq)
         (map #(assoc %1
                 :name (hyphenize-keyword (:column-name %1))
                 :type (hyphenize-keyword (lower-case (:type-name %1))))))))

(defn catalogs
  "Retrieves the catalog names available in this database."
  [db]
  (with-connection [db db]
    (->> (.getCatalogs (.getMetaData (:connection db)))
         (resultset-seq)
         (map #(assoc %1 :name (hyphenize-keyword (:table-cat %1)))))))

(defn columns
  "Retrieves a description of the database columns matching `catalog`,
  `schema`, `table` and `name`."
  [db & {:keys [catalog schema table name entities]}]
  (with-connection [db db]
    (->> (.getColumns
          (.getMetaData (:connection db))
          (if catalog (sql-name db catalog))
          (if schema (sql-name db schema))
          (if table (sql-name db table))
          (if name (sql-name db name)))
         (resultset-seq)
         (map #(assoc %1
                 :catalog (hyphenize-keyword (:table-cat %1))
                 :schema (hyphenize-keyword (:table-schem %1))
                 :table (hyphenize-keyword (:table-name %1))
                 :name (hyphenize-keyword (:column-name %1))
                 :type (hyphenize-keyword (lower-case (:type-name %1))))))))

(defn indexes
  "Retrieves a description of the given table's primary key columns."
  [db & {:keys [catalog schema table unique approximate entities]}]
  (with-connection [db db]
    (->> (.getIndexInfo
          (.getMetaData (:connection db))
          (if catalog (sql-name db catalog))
          (if schema (sql-name db schema))
          (if table (sql-name db table))
          (= true unique)
          (= true approximate))
         (resultset-seq)
         (map #(assoc %1
                 :catalog (hyphenize-keyword (:table-cat %1))
                 :schema (hyphenize-keyword (:table-schem %1))
                 :table (hyphenize-keyword (:table-name %1)))))))

(defn primary-keys
  "Retrieves a description of the given table's primary key columns."
  [db & {:keys [catalog schema table entities]}]
  (with-connection [db db]
    (->> (.getPrimaryKeys
          (.getMetaData (:connection db))
          (if catalog (sql-name db catalog))
          (if schema (sql-name db schema))
          (if table (sql-name db table)))
         (resultset-seq)
         (map #(assoc %1
                 :catalog (hyphenize-keyword (:table-cat %1))
                 :schema (hyphenize-keyword (:table-schem %1))
                 :table (hyphenize-keyword (:table-name %1))
                 :name (hyphenize-keyword (:column-name %1)))))))

(defn unique-columns [db & {:keys [catalog schema table name entities]}]
  (let [indexes (indexes db :catalog catalog :schema schema :table table :name name :unique true :entities entities)
        indexes (set (map #(vector (:table-name %1) (:column-name %1)) indexes))]
    (filter #(contains? indexes [(:table-name %1) (:column-name %1)])
            (columns db :catalog catalog :schema schema :table table :name name :entities entities))))

(defn schemas
  "Retrieves the catalog names available in this database."
  [db]
  (with-connection [db db]
    (->> (.getSchemas (.getMetaData (:connection db)))
         (resultset-seq)
         (map #(assoc %1 :name (hyphenize-keyword (:table-schem %1)))))))

(defn tables
  "Retrieves a description of the database tables matching `catalog`,
  `schema`, `name` and `types`."
  [db & {:keys [catalog schema name types entities]}]
  (with-connection [db db]
    (->> (.getTables
          (.getMetaData (:connection db))
          (if catalog (sql-name db catalog))
          (if schema (sql-name db schema))
          (if name (sql-name db name))
          (into-array String (or types ["TABLE"])))
         (resultset-seq)
         (map #(assoc %1
                 :catalog (hyphenize-keyword (:table-cat %1))
                 :schema (hyphenize-keyword (:table-schem %1))
                 :name (hyphenize-keyword (:table-name %1))
                 :type (hyphenize-keyword (lower-case (:table-type %1))))))))

(defn views
  "Retrieves a description of the database views matching `catalog`,
  `schema` and `name`."
  [db & {:keys [catalog schema name types entities]}]
  (tables db :catalog catalog :schema schema :name name :types ["VIEW"] :entities entities))
