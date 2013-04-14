(ns datumbazo.meta
  (:import java.sql.DatabaseMetaData)
  (:refer-clojure :exclude [resultset-seq])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [lower-case]]
            [inflections.core :refer [hyphenize hyphenize-keys underscore]]
            [sqlingvo.util :refer [as-identifier]]))

(defn- hyphenize-keyword [k]
  (if k (keyword (hyphenize (name k)))))

(defn- resultset-seq [^java.sql.ResultSet rs]
  (->> (clojure.core/resultset-seq rs)
       (map #(hyphenize-keys (merge {} %1)))))

(defn metadata
  "Returns the DatabaseMetaData object for `db`."
  [db]
  (assert db (format "Can't get database meta data: %s" db))
  (.getMetaData (jdbc/get-connection db)))

(defn best-row-identifiers
  "Retrieves a description of a table's optimal set of columns that uniquely identifies a row."
  [db & {:keys [catalog schema table scope nullable entities]}]
  (jdbc/with-naming-strategy {:entity (or entities underscore)}
    (->> (.getBestRowIdentifier
          (metadata db)
          (if catalog (as-identifier catalog))
          (if schema (as-identifier schema))
          (if table (as-identifier table))
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
  (->> (.getCatalogs (metadata db))
       (resultset-seq)
       (map #(assoc %1 :name (hyphenize-keyword (:table-cat %1))))))

(defn columns
  "Retrieves a description of the database columns matching `catalog`,
  `schema`, `table` and `name`."
  [db & {:keys [catalog schema table name entities]}]
  (jdbc/with-naming-strategy {:entity (or entities underscore)}
    (->> (.getColumns
          (metadata db)
          (if catalog (as-identifier catalog))
          (if schema (as-identifier schema))
          (if table (as-identifier table))
          (if name (as-identifier name)))
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
  (jdbc/with-naming-strategy {:entity (or entities underscore)}
    (->> (.getIndexInfo
          (metadata db)
          (if catalog (as-identifier catalog))
          (if schema (as-identifier schema))
          (if table (as-identifier table))
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
  (jdbc/with-naming-strategy {:entity (or entities underscore)}
    (->> (.getPrimaryKeys
          (metadata db)
          (if catalog (as-identifier catalog))
          (if schema (as-identifier schema))
          (if table (as-identifier table)))
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
  (->> (.getSchemas (metadata db))
       (resultset-seq)
       (map #(assoc %1 :name (hyphenize-keyword (:table-schem %1))))))

(defn tables
  "Retrieves a description of the database tables matching `catalog`,
  `schema`, `name` and `types`."
  [db & {:keys [catalog schema name types entities]}]
  (jdbc/with-naming-strategy {:entity (or entities underscore)}
    (->> (.getTables
          (metadata db)
          (if catalog (as-identifier catalog))
          (if schema (as-identifier schema))
          (if name (as-identifier name))
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
