(ns datumbazo.core
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.associations :as associations]
            [datumbazo.driver.core :as driver]
            [datumbazo.pagination :as pagination]
            [datumbazo.table :as table]
            [datumbazo.db :as db]
            [datumbazo.pool.core :as pool]
            [datumbazo.util :refer [immigrate]]
            [no.en.core :refer [parse-integer]]
            [potemkin :refer [import-vars]]
            [sqlingvo.core :as sql]))

(import-vars
 [datumbazo.associations
  belongs-to
  has-many]
 [datumbazo.driver.core
  begin
  commit
  connect
  connection
  disconnect
  execute
  prepare-statement
  rollback
  sql-str
  with-connection
  with-transaction]
 [datumbazo.table
  columns
  deftable
  table]
 [datumbazo.db
  new-db
  with-db]
 [datumbazo.pagination
  page-info
  paginate]
 [datumbazo.util
  make-instance
  make-instances])

(immigrate 'sqlingvo.core)

(declare run)

(defn print-explain
  "Print the execution plan of `query`."
  [query]
  (doseq [row @(explain (-> query ast :db) query)]
    (println (get row (keyword "query plan")))))

(defn count-all
  "Count all rows in the database `table`."
  [db table]
  (->> @(sql/select db ['(count *)]
          (sql/from table))
       first :count))

(driver/load-drivers)
(pool/load-connection-pools)
