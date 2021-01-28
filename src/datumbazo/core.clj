(ns datumbazo.core
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.datasource :as pool]
            [datumbazo.db :as db]
            [datumbazo.driver.core :as driver]
            [datumbazo.pagination :as pagination]
            [datumbazo.postgresql.associations :as associations]
            [datumbazo.table :as table]
            [datumbazo.util :refer [immigrate]]
            [no.en.core :refer [parse-integer]]
            [potemkin :refer [import-vars]]
            [sqlingvo.core :as sql]))

(immigrate 'sqlingvo.core)

(import-vars
 [datumbazo.postgresql.associations
  belongs-to
  has-and-belongs-to-many
  has-many
  has-one]
 [datumbazo.driver.core
  auto-commit?
  set-auto-commit!
  set-savepoint!
  connect
  connected?
  connection
  disconnect
  execute
  prepare
  rollback!
  sql-str
  transact
  with-connection
  with-rollback
  with-savepoint
  with-transaction]
 [datumbazo.table
  columns
  deftable
  table]
 [datumbazo.db
  db
  with-db]
 [datumbazo.pagination
  page-info
  paginate]
 [datumbazo.util
  make-instance
  make-instances])

(declare run)

(defn print-explain
  "Print the execution plan of `query`."
  [query]
  (doseq [row @(explain (-> query ast :db) query)]
    (println (get row (keyword "QUERY PLAN")))))

(defn count-all
  "Count all rows in the database `table`."
  [db table]
  (->> @(sql/select db ['(count *)]
          (sql/from table))
       first :count))
