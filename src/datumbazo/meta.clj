(ns datumbazo.meta
  (:import java.sql.DatabaseMetaData)
  (:refer-clojure :exclude [resultset-seq])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [lower-case]]
            [datumbazo.util :refer [defn-memo]]
            [inflections.core :refer [hyphenize hyphenize-keys]]))

(defn hyphenize-keyword [k]
  (if k (keyword (hyphenize (name k)))))

(defn- resultset-seq [^java.sql.ResultSet rs]
  (->> (clojure.core/resultset-seq rs)
       (map #(hyphenize-keys (merge {} %1)))))

(defn metadata
  "Returns the DatabaseMetaData object for `connection`."
  [connection]
  (assert connection (format "Can't get database meta data: %s" connection))
  (.getMetaData connection))

(defn best-row-identifiers
  "Retrieves a description of a table's optimal set of columns that uniquely identifies a row."
  [connection & {:keys [catalog schema table scope nullable]}]
  (->> (.getBestRowIdentifier
        (metadata connection)
        (if catalog (jdbc/as-identifier catalog))
        (if schema (jdbc/as-identifier schema))
        (if table (jdbc/as-identifier table))
        (condp = scope
          :temporary DatabaseMetaData/bestRowTemporary
          :transaction DatabaseMetaData/bestRowTransaction
          :session DatabaseMetaData/bestRowSession
          DatabaseMetaData/bestRowTemporary)
        (nil? nullable))
       (resultset-seq)
       (map #(assoc %1
               :name (hyphenize-keyword (:column-name %1))
               :type (hyphenize-keyword (lower-case (:type-name %1)))))))

(defn-memo catalogs
  "Retrieves the catalog names available in this database."
  [connection]
  (->> (.getCatalogs (metadata connection))
       (resultset-seq)
       (map #(assoc %1 :name (hyphenize-keyword (:table-cat %1))))))

(defn-memo columns
  "Retrieves a description of the database columns matching `catalog`,
  `schema`, `table` and `name`."
  [connection & {:keys [catalog schema table name]}]
  (->> (.getColumns
        (metadata connection)
        (if catalog (jdbc/as-identifier catalog))
        (if schema (jdbc/as-identifier schema))
        (if table (jdbc/as-identifier table))
        (if name (jdbc/as-identifier name)))
       (resultset-seq)
       (map #(assoc %1
               :catalog (hyphenize-keyword (:table-cat %1))
               :schema (hyphenize-keyword (:table-schem %1))
               :table (hyphenize-keyword (:table-name %1))
               :name (hyphenize-keyword (:column-name %1))
               :type (hyphenize-keyword (lower-case (:type-name %1)))))))

(defn-memo tables
  "Retrieves a description of the database tables matching `catalog`,
  `schema`, `name` and `types`."
  [connection & {:keys [catalog schema name types]}]
  (->> (.getTables
        (metadata connection)
        (if catalog (jdbc/as-identifier catalog))
        (if schema (jdbc/as-identifier schema))
        (if name (jdbc/as-identifier name))
        (into-array String (or types ["TABLE"])))
       (resultset-seq)
       (map #(assoc %1
               :catalog (hyphenize-keyword (:table-cat %1))
               :schema (hyphenize-keyword (:table-schem %1))
               :name (hyphenize-keyword (:table-name %1))
               :type (hyphenize-keyword (lower-case (:table-type %1)))))))

(defn primary-keys
  "Retrieves a description of the given table's primary key columns."
  [connection & {:keys [catalog schema table]}]
  (->> (.getPrimaryKeys
        (metadata connection)
        (if catalog (jdbc/as-identifier catalog))
        (if schema (jdbc/as-identifier schema))
        (if table (jdbc/as-identifier table)))
       (resultset-seq)
       (map #(assoc %1
               :catalog (hyphenize-keyword (:table-cat %1))
               :schema (hyphenize-keyword (:table-schem %1))
               :table (hyphenize-keyword (:table-name %1))
               :name (hyphenize-keyword (:column-name %1))))))

(defn-memo schemas
  "Retrieves the catalog names available in this database."
  [connection]
  (->> (.getSchemas (metadata connection))
       (resultset-seq)
       (map #(assoc %1 :name (hyphenize-keyword (:table-schem %1))))))

(defn-memo views
  "Retrieves a description of the database views matching `catalog`,
  `schema` and `name`."
  [connection & {:keys [catalog schema name types]}]
  (tables connection :catalog catalog :schema schema :name name :types ["VIEW"]))