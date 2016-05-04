(ns datumbazo.db
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as driver]
            [datumbazo.driver.test :as test-driver]
            [datumbazo.pool.core :as pool]
            [no.en.core :as noencore]
            [sqlingvo.db :as db]))

(def ^:private jdbc-url-regex
  "The regular expression to match JDBC urls."
  #"(([^:]+):)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))")

(defn parse-url
  "Parse the database `url` and return a Ring compatible map."
  [url]
  (if-let [matches (re-matches jdbc-url-regex (str url))]
    (let [database (nth matches 13)
          server-name (nth matches 8)
          server-port (noencore/parse-integer (nth matches 10))
          query-string (nth matches 15)]
      {:name database
       :password (nth matches 6)
       :pool (keyword (nth matches 2))
       :query-params (noencore/parse-query-params query-string)
       :scheme (keyword (nth matches 3))
       :server-name server-name
       :server-port server-port
       :user (nth matches 5)})
    (throw (ex-info "Can't parse JDBC url %s." {:url url}))))

(defn format-url
  "Format the `db` spec as a URL."
  [db]
  (let [spec (assoc db :uri (str "/" (:name db)))]
    (noencore/format-url spec)))

(defn- assoc-driver [db]
  (->> (if (:test? db)
         (test-driver/driver db)
         (driver/find-driver db))
       (assoc db :driver)))

(defn new-db
  "Return a new database from `spec`."
  [spec & [opts]]
  (-> (merge {:backend 'clojure.java.jdbc
              :eval-fn #'driver/execute}
             (if (map? spec)
               spec
               (parse-url spec)))
      (db/db)
      (merge opts)
      (assoc-driver)))

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

(defmacro with-db
  "Start a database component using `config` bind it to `db-sym`,
  evaluate `body` and close the database connection again."
  [[db-sym config & [opts]] & body]
  `(let [db# (new-db ~config ~opts)
         component# (component/start db#)
         ~db-sym component#]
     (try ~@body
          (finally (component/stop component#)))))
