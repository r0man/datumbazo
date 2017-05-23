(ns datumbazo.core
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.associations :as associations]
            [datumbazo.driver.core :as driver]
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
 [datumbazo.util
  make-instance
  make-instances])

(immigrate 'sqlingvo.core)

(def ^:dynamic *page* nil)
(def ^:dynamic *per-page* 25)

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

(defn paginate
  "Add LIMIT and OFFSET clauses to `query` calculated from `page` and
  `per-page.`"
  [page & [limit per-page]]
  (let [page (parse-integer (or page *page*))
        per-page (parse-integer (or limit per-page *per-page*))]
    (fn [stmt]
      (if page
        ((chain-state
          [(sqlingvo.core/limit per-page)
           (offset (* (dec page) (or per-page *per-page*)))])
         stmt)
        [nil stmt]))))

(driver/load-drivers)
(pool/load-connection-pools)
