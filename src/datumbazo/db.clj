(ns datumbazo.db
  (:require [com.stuartsierra.component :as component]
            [datumbazo.datasource :as pool]
            [datumbazo.driver.core :as driver]
            [datumbazo.driver.test :as test-driver]
            [sqlingvo.db :as db]))

(defn- assoc-driver [db]
  (when (contains? db :test?)
    (println "Deprecation warning: Use :rollback-only option instead of :test?"))
  (->> (if (or (:test? db) (:rollback-only db))
         (test-driver/driver db)
         (driver/driver db))
       (assoc db :driver)))

(defn db
  "Return a new database from `spec`."
  [spec & [opts]]
  (->> (merge
        {:backend :clojure.java.jdbc
         :eval-fn #'driver/execute} opts)
       (db/db spec)
       (assoc-driver)))

(defmacro with-db
  "Start a database component using `config` bind it to `db-sym`,
  evaluate `body` and close the database connection again."
  [[db-sym config & [opts]] & body]
  `(let [db# (db ~config ~opts)
         component# (component/start db#)
         ~db-sym component#]
     (try ~@body
          (finally (component/stop component#)))))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [db]
    (cond-> db
      (:pool db) (pool/start-datasource)
      true (update :driver component/start)))
  (stop [db]
    (-> (update db :driver component/stop)
        (pool/stop-datasource))))
