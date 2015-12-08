(ns datumbazo.db
  (:require [clojure.string :refer [blank?]]
            [datumbazo.connection :refer [run*]]
            [datumbazo.driver.core :refer [eval-db]]
            [datumbazo.util :as util]
            [datumbazo.vendor :as vendor]
            [no.en.core :refer [parse-integer parse-query-params]]
            [sqlingvo.db :as db]))

(def ^:private jdbc-url-regex
  #"(([^:]+):)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))")

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
       :password (nth matches 6)})
    (throw (ex-info "Can't parse JDBC url %s." {:url url}))))

(defn new-db [spec]
  (let [spec (if (map? spec) spec (parse-url spec))]
    (assoc (vendor/db-spec spec)
           :backend 'clojure.java.jdbc
           :eval-fn #'eval-db)))
