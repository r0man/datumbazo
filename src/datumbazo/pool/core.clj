(ns datumbazo.pool.core
  (:require [com.stuartsierra.component :as component]))

(defmulti db-pool
  "Return a database connection pool for `db-spec`."
  (fn [db-spec & [opts]] (:pool db-spec)))

;; TODO: Remove this soon.
(defmethod db-pool :jdbc [db-spec & [opts]]
  nil)

(defmethod db-pool :default [db-spec & [opts]]
  (throw (ex-info (str "Unsupported connection pool: " (:pool db-spec)) db-spec)))

(defn assoc-pool
  "Assoc a database connection pool onto `db-spec`."
  [db-spec]
  (if-let [pool (db-pool db-spec)]
    (assoc db-spec :datasource pool)
    db-spec))
