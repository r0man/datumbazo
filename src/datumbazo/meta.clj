(ns datumbazo.meta
  (:refer-clojure :exclude [resultset-seq])
  (:require [clojure.string :refer [lower-case]]
            [datumbazo.driver.core :refer [connection with-connection]]
            [inflections.core :refer [hyphenate hyphenate-keys]]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]
            [sqlingvo.util :refer [sql-name]])
  (:import java.sql.DatabaseMetaData))

(defn current-schema
  "Returns the current schema from `db`."
  [db]
  (-> @(sql/select db ['(current_schema)]) ffirst second))

(defn- hyphenate-keyword [k]
  (if k (keyword (hyphenate (name k)))))

(defn- resultset-seq [^java.sql.ResultSet rs]
  (->> (clojure.core/resultset-seq rs)
       (mapv #(hyphenate-keys (merge {} %1)))))

(defmacro with-metadata
  "Get the metadata from the `db` connection, bind it to
  `metadata-sym` and evaluate `body`."
  [[metadata-sym db] & body]
  `(with-connection [db# ~db]
     (let [~metadata-sym (.getMetaData (connection db#))]
       ~@body)))

(defn best-row-identifiers
  "Retrieves a description of a table's optimal set of columns that uniquely identifies a row."
  [db & [{:keys [catalog schema table scope nullable]}]]
  (with-metadata [metadata db]
    (->> (.getBestRowIdentifier
          metadata
          (sql-name db catalog)
          (sql-name db schema)
          (sql-name db table)
          (condp = scope
            :temporary DatabaseMetaData/bestRowTemporary
            :transaction DatabaseMetaData/bestRowTransaction
            :session DatabaseMetaData/bestRowSession
            DatabaseMetaData/bestRowTemporary)
          (not (nil? nullable)))
         (resultset-seq)
         (mapv #(assoc %1
                       :name (hyphenate-keyword (:column-name %1))
                       :type (hyphenate-keyword (lower-case (:type-name %1))))))))

(defn catalogs
  "Retrieves the catalog names available in this database."
  [db]
  (with-metadata [metadata db]
    (->> (.getCatalogs metadata)
         (resultset-seq)
         (mapv #(assoc %1 :name (hyphenate-keyword (:table-cat %1)))))))

(defn columns
  "Retrieves a description of the database columns matching `catalog`,
  `schema`, `table` and `name`."
  [db & [{:keys [catalog schema table name]}]]
  (with-metadata [metadata db]
    (->> (.getColumns
          metadata
          (sql-name db catalog)
          (sql-name db schema)
          (sql-name db table)
          (sql-name db name))
         (resultset-seq)
         (mapv #(assoc %1
                       :catalog (hyphenate-keyword (:table-cat %1))
                       :schema (hyphenate-keyword (:table-schem %1))
                       :table (hyphenate-keyword (:table-name %1))
                       :name (hyphenate-keyword (:column-name %1))
                       :type (hyphenate-keyword (lower-case (:type-name %1))))))))

(defn column
  "Returns the information schema for `column` in `db`."
  [db column]
  (some->> (expr/parse-column column)
           (columns db)
           (first)))

(defn indexes
  "Retrieves a description of the given table's primary key columns."
  [db & [{:keys [catalog schema table unique approximate]}]]
  (with-metadata [metadata db]
    (->> (.getIndexInfo
          metadata
          (sql-name db catalog)
          (sql-name db schema)
          (sql-name db table)
          (= true unique)
          (= true approximate))
         (resultset-seq)
         (mapv #(assoc %1
                       :catalog (hyphenate-keyword (:table-cat %1))
                       :schema (hyphenate-keyword (:table-schem %1))
                       :table (hyphenate-keyword (:table-name %1)))))))

(defn primary-keys
  "Retrieves a description of the given table's primary key columns."
  [db & [{:keys [catalog schema table]}]]
  (with-metadata [metadata db]
    (->> (.getPrimaryKeys
          metadata
          (sql-name db catalog)
          (sql-name db schema)
          (sql-name db table))
         (resultset-seq)
         (mapv #(assoc %1
                       :catalog (hyphenate-keyword (:table-cat %1))
                       :schema (hyphenate-keyword (:table-schem %1))
                       :table (hyphenate-keyword (:table-name %1))
                       :name (hyphenate-keyword (:column-name %1)))))))

(defn unique-columns
  "Retrieves the unique columns of a table."
  [db & [{:keys [catalog schema table name]}]]
  (let [indexes (indexes
                 db {:catalog catalog
                     :schema schema
                     :table table
                     :name name
                     :unique true})
        indexes (set (map #(vector (:table-name %1) (:column-name %1)) indexes))]
    (filter #(contains? indexes [(:table-name %1) (:column-name %1)])
            (columns db {:catalog catalog
                         :schema schema
                         :table table
                         :name name}))))

(defn schemas
  "Retrieves the catalog names available in this database."
  [db]
  (with-metadata [metadata db]
    (->> (.getSchemas metadata)
         (resultset-seq)
         (mapv #(assoc %1 :name (hyphenate-keyword (:table-schem %1)))))))

(defn tables
  "Retrieves a description of the database tables matching `catalog`,
  `schema`, `name` and `types`."
  [db & [{:keys [catalog schema name types]}]]
  (with-metadata [metadata db]
    (->> (.getTables
          metadata
          (sql-name db catalog)
          (sql-name db schema)
          (sql-name db name)
          (into-array String (or types ["TABLE"])))
         (resultset-seq)
         (mapv #(assoc %1
                       :catalog (hyphenate-keyword (:table-cat %1))
                       :schema (hyphenate-keyword (:table-schem %1))
                       :name (hyphenate-keyword (:table-name %1))
                       :type (hyphenate-keyword (lower-case (:table-type %1))))))))

(defn views
  "Retrieves a description of the database views matching `catalog`,
  `schema` and `name`."
  [db & [{:keys [catalog schema name types]}]]
  (tables db {:catalog catalog
              :schema schema
              :name name
              :types ["VIEW"]}))

(defn table
  "Returns the information schema for `table` in `db`."
  [db table]
  (when-let [{:keys [schema name]} (expr/parse-table table)]
    {:primary-keys (primary-keys db {:schema schema :table name})
     :unique-columns (unique-columns db {:schema schema :table name})}))
