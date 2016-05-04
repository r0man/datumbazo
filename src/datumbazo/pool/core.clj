(ns datumbazo.pool.core
  (:require [clojure.string :as str]))

(defmulti db-pool
  "Return a database connection pool for `db`."
  (fn [db & [opts]] (:pool db)))

(defmethod db-pool :default [db & [opts]]
  (throw (ex-info (str "Unsupported connection pool: " (:pool db))
                  (into {} db))))

(defn assoc-pool
  "Assoc a database connection pool onto `db`."
  [db]
  (if-let [pool (db-pool db)]
    (assoc db :datasource pool)
    db))

(defn load-connection-pools
  "Load connection pool support."
  []
  (doseq [ns '[datumbazo.pool.bonecp
               datumbazo.pool.c3p0
               datumbazo.pool.hikaricp]]
    (try (require ns) (catch Exception e))))
