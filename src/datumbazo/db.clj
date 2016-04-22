(ns datumbazo.db
  (:require [datumbazo.driver.core :as driver]
            [datumbazo.pool.core :as pool]
            [no.en.core :refer [parse-integer parse-query-params]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [sqlingvo.db :as db]
            [com.stuartsierra.component :as component]))

(def ^:private jdbc-url-regex
  "The regular expression to match JDBC urls."
  #"(([^:]+):)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))")

(defn- format-server [db-spec]
  (str (:host db-spec)
       (if (:port db-spec)
         (str ":" (:port db-spec)))))

(defmulti subname
  "Return the JDBC subname for `db-spec`."
  (fn [db-spec]
    (keyword (or (:subprotocol db-spec)
                 (:scheme db-spec)))))

(defmethod subname :oracle [db-spec]
  (str ":" (:user db-spec) "/" (:password db-spec) "@"
       (format-server db-spec)
       ":" (:name db-spec)))

(defmethod subname :sqlserver [db-spec]
  (str "//" (format-server db-spec) ";"
       "database=" (:name db-spec) ";"
       "user=" (:user db-spec) ";"
       "password=" (:password db-spec)))

(defmethod subname :default [db-spec]
  (str "//" (:server-name db-spec)
       (if-let [port (:server-port db-spec)]
         (str ":" port))
       "/" (:name db-spec)
       (if-not (str/blank? (:query-string db-spec))
         (str "?" (:query-string db-spec)))))

(defn parse-url
  "Parse the database `url` and return a Ring compatible map."
  [url]
  (if-let [matches (re-matches jdbc-url-regex (str url))]
    (let [database (nth matches 13)
          server-name (nth matches 8)
          server-port (parse-integer (nth matches 10))
          query-string (nth matches 15)]
      (as-> {:name database
             :host server-name
             :scheme (keyword (nth matches 3))
             :server-name server-name
             :server-port server-port
             :params (parse-query-params query-string)
             :query-params (parse-query-params query-string)
             :pool (keyword (or (nth matches 2) :jdbc))
             :port server-port
             :query-string query-string
             :uri (nth matches 12)
             :subprotocol (nth matches 3)
             :user (nth matches 5)
             :password (nth matches 6)}
          db-spec
        (assoc db-spec :subname (subname db-spec))))
    (throw (ex-info "Can't parse JDBC url %s." {:url url}))))

(defn new-db
  "Return a new database from `spec`."
  [spec]
  (->> (if (map? spec) spec (parse-url spec))
       (merge {:backend 'clojure.java.jdbc
               :eval-fn #'driver/eval-db})
       (db/db)))

;; (extend-type sqlingvo.db.Database
;;   component/Lifecycle
;;   (start [db]
;;     (let [db (start-db db)]
;;       (if (:rollback? db)
;;         (driver/begin db)
;;         db)))
;;   (stop [db]
;;     (when (:rollback? db)
;;       (driver/rollback! db))
;;     (stop-db db)))

(extend-type sqlingvo.db.Database
  component/Lifecycle
  (start [db]
    (if (:pool db)
      (pool/assoc-pool db)
      db))
  (stop [db]
    (when-let [datasource (:datasource db)]
      (.close datasource))
    (assoc db :datasource nil)))

(defn- load-connection-pools
  "Load connection pool support."
  []
  (doseq [ns '[datumbazo.pool.bonecp
               datumbazo.pool.c3p0
               datumbazo.pool.hikaricp]
          :let [product (str/capitalize (last (str/split (name ns) #"\.")))]]
    (try (require ns)
         (catch Exception e
           (log/infof "Can't load %s connection pool support. %s" product (.getMessage e))))))

(load-connection-pools)
