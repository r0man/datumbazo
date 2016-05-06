(ns datumbazo.db
  (:require [com.stuartsierra.component :as component]
            [datumbazo.connection :refer [execute]]
            [datumbazo.driver.core :as driver]
            [datumbazo.driver.test :as test-driver]
            [datumbazo.pool.core :as pool]
            [datumbazo.util :as util]
            [sqlingvo.db :as db]))

(defn- assoc-driver [db]
  (->> (if (:test? db)
         (test-driver/driver db)
         (driver/find-driver db))
       (assoc db :driver)))

(defn new-db
  "Return a new database from `spec`."
  [spec & [opts]]
  (-> (merge {:backend 'clojure.java.jdbc
              :eval-fn #'execute}
             (if (map? spec)
               spec
               (util/parse-url spec)))
      (db/db)
      (merge opts)
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
      (:pool db) (pool/assoc-pool)
      true (update :driver component/start)))
  (stop [db]
    (when-let [datasource (:datasource db)]
      (.close datasource))
    (-> (update db :driver component/stop)
        (assoc :datasource nil))))
