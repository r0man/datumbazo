(ns datumbazo.db
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as driver]
            [datumbazo.driver.test :as test-driver]
            [datumbazo.pool.core :as pool]
            [sqlingvo.db :as db]))

(defn- assoc-driver [db]
  (->> (if (or (:test? db) (:rollback db))
         (test-driver/driver db)
         (driver/find-driver db))
       (assoc db :driver)))

(defn new-db
  "Return a new database from `spec`."
  [spec & [opts]]
  (->> (merge
        {:backend 'clojure.java.jdbc
         :eval-fn #'driver/execute} opts)
       (db/db spec)
       (assoc-driver)))

(defmacro with-db
  "Start a database component using `config` bind it to `db-sym`,
  evaluate `body` and close the database connection again."
  [[db-sym config & [opts]] & body]
  `(let [db# (new-db ~config ~opts)
         component# (component/start db#)
         ~db-sym component#]
     (try ~@body
          (finally (component/stop component#)))))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [db]
    (cond-> db
      (:pool db) (pool/start-pool)
      true (update :driver component/start)))
  (stop [db]
    (-> (update db :driver component/stop)
        (pool/stop-pool))))
