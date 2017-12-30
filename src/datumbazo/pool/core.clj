(ns datumbazo.pool.core
  (:require [clojure.string :as str]))

(defmulti db-pool
  "Return a database connection pool for `db`."
  (fn [db & [opts]] (:pool db)))

(defmethod db-pool :default [db & [opts]]
  (throw (ex-info (str "Unsupported connection pool: " (:pool db))
                  (into {} db))))

(defn start-pool
  "Start a connection pool and assoc it onto the :driver of `db`."
  [db]
  (if-let [pool (db-pool db)]
    (assoc db :datasource pool)
    db))

(defn stop-pool
  "Stop the connection pool and dissoc it from the :driver of `db`."
  [db]
  (some-> db :datasource .close)
  (assoc db :datasource nil))

(defn load-connection-pools
  "Load connection pool support."
  []
  (doseq [ns '[datumbazo.pool.bonecp
               datumbazo.pool.c3p0
               datumbazo.pool.hikaricp]]
    (try (require ns) (catch Exception e))))
