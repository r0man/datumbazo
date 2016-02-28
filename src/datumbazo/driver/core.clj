(ns datumbazo.driver.core
  (:require [sqlingvo.core :refer [ast sql]]))

(defmulti apply-transaction
  "Apply `f` within a database transaction"
  (fn [db f & [opts]] (:backend db)))

(defmulti close-db
  "Close the connection to `db`."
  (fn [db] (:backend db)))

(defmulti fetch
  "Execute `sql` and return rows."
  (fn [db sql & [opts]] (:backend db)))

(defmulti execute
  "Execute `sql` and return the number of affected rows."
  (fn [db sql & [opts]] (:backend db)))

(defmulti open-db
  "Open a connection to `db`."
  (fn [db] (:backend db)))

(defn eval-db
  "Eval the `stmt` against a database."
  [{:keys [db] :as ast} & [opts]]
  (let [sql (sql ast)]
    (case (:op ast)
      :delete
      (if (:returning ast)
        (fetch db sql)
        (execute db sql))
      :except
      (fetch db sql)
      :explain
      (fetch db sql)
      :insert
      (if (:returning ast)
        (fetch db sql)
        (execute db sql))
      :intersect
      (fetch db sql)
      :select
      (fetch db sql)
      :union
      (fetch db sql)
      :update
      (if (:returning ast)
        (fetch db sql)
        (execute db sql))
      (execute db sql))))

(defn row-count
  "Normalize into a record, with the count of affected rows."
  [result]
  [{:count
    (if (sequential? result)
      (first result)
      result)}])
