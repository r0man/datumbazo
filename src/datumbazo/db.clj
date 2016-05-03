(ns datumbazo.db
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as driver]
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

(defn new-db
  "Return a new database from `spec`."
  [spec]
  (->> (if (map? spec) spec (parse-url spec))
       (merge {:backend 'clojure.java.jdbc
               :eval-fn #'driver/eval-db})
       (db/db)))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [db]
    (cond-> db
      (:pool db) (pool/assoc-pool)
      (:rollback? db) (driver/open-test-connection)))
  (stop [db]
    (when (:rollback? db)
      (driver/close-test-connection db))
    (when-let [datasource (:datasource db)]
      (.close datasource))
    (assoc db :connection nil :test-connection nil :datasource nil)))

(defmacro with-db
  "Start a database component using `config` bind it to `db-sym`,
  evaluate `body` and close the database connection again."
  [[db-sym config & [opts]] & body]
  `(let [db# (merge (new-db ~config) ~opts)
         component# (component/start db#)
         ~db-sym component#]
     (try ~@body
          (finally (component/stop component#)))))
