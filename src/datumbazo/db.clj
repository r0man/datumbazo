(ns datumbazo.db
  (:require [clojure.string :refer [blank?]]
            [datumbazo.util :as util]
            [no.en.core :refer [parse-integer parse-query-params]]
            [sqlingvo.db :as db]))

(def ^:private jdbc-url-regex
  #"(([^:]+):)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))")

(defn- format-server [spec]
  (str (:host spec)
       (if (:port spec)
         (str ":" (:port spec)))))

(defn- subname [spec]
  (str "//" (:server-name spec)
       (if-let [port (:server-port spec)]
         (str ":" port))
       "/" (:name spec)
       (if-not (blank? (:query-string spec))
         (str "?" (:query-string spec)))))

(defn parse-url
  "Parse the database url `s` and return a Ring compatible map."
  [url]
  (if-let [matches (re-matches jdbc-url-regex (str url))]
    (let [database (nth matches 13)
          server-name (nth matches 8)
          server-port (parse-integer (nth matches 10))
          query-string (nth matches 15)]
      {:name database
       :host server-name
       :server-name server-name
       :server-port server-port
       :params (parse-query-params query-string)
       :pool (keyword (or (nth matches 2) :jdbc))
       :port server-port
       :query-string query-string
       :uri (nth matches 12)
       :subprotocol (nth matches 3)
       :user (nth matches 5)
       :username (nth matches 5)
       :password (nth matches 6)})
    (throw (ex-info "Can't parse JDBC url %s." {:url url}))))

(defmulti enhance-spec
  (fn [spec]
    (or (keyword (:subprotocol spec))
        (:scheme spec))))

(defmethod enhance-spec :mysql [spec]
  (assoc (db/mysql spec)
    :subname (subname spec)))

(defmethod enhance-spec :oracle [spec]
  (db/oracle
   (assoc spec
     :subprotocol "oracle:thin"
     :subname (str ":" (:username spec) "/" (:password spec) "@"
                   (format-server spec)
                   ":" (:name spec)))))

(defmethod enhance-spec :postgresql [spec]
  (assoc (db/postgresql spec)
    :subname (subname spec)))

(defmethod enhance-spec :vertica [spec]
  (assoc (db/vertica spec)
    :subname (subname spec)))

(defmethod enhance-spec :sqlite [spec]
  (assoc (db/sqlite spec)
    :subname (subname spec)))

(defmethod enhance-spec :sqlserver [spec]
  (db/sqlserver
   (assoc spec
     :subname (str "//" (format-server spec) ";"
                   "database=" (:name spec) ";"
                   "user=" (:username spec) ";"
                   "password=" (:password spec)))))

(defn new-db [spec]
  (enhance-spec (if (map? spec) spec (parse-url spec))))
