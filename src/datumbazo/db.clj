(ns datumbazo.db
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as driver]
            [datumbazo.driver.test :as test-driver]
            [datumbazo.pool.core :as pool]
            [sqlingvo.db :as db]))

(defn- assoc-driver [db]
  (->> (if (:test? db)
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
  driver/IConnection
  (-connect [db opts]
    (update db :driver #(driver/-connect % opts)))
  (-connection [db]
    (driver/-connection (:driver db)))
  (-disconnect [db]
    (update db :driver driver/-disconnect))

  driver/IExecute
  (-execute [db sql opts]
    (driver/-execute (:driver db) sql opts))

  driver/IFetch
  (-fetch [db sql opts]
    (driver/-fetch (:driver db) sql opts))

  driver/IPrepareStatement
  (-prepare-statement [db sql opts]
    (driver/-prepare-statement (:driver db) sql opts))

  driver/ITransaction
  (-begin [db opts]
    (update db :driver #(driver/-begin % opts)))
  (-commit [db opts]
    (update db :driver #(driver/-commit % opts)))
  (-rollback [db opts]
    (update db :driver #(driver/-rollback % opts)))

  component/Lifecycle
  (start [db]
    (cond-> db
      (:pool db) (pool/assoc-pool)
      true (update :driver component/start)))
  (stop [db]
    (some-> db :datasource .close)
    (-> (update db :driver component/stop)
        (assoc :datasource nil))))
