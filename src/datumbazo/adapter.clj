(ns datumbazo.adapter
  (:require [datumbazo.io :as io]
            [datumbazo.util :as util]
            [clojure.java.jdbc :as jdbc]
            [sqlingvo.core :as sql]))

(defn- columns
  "Returns the columns of `table`."
  [table] (map (:column table) (:columns table)))

(defn- prepare
  "Add the preparation fn `f` to `table`."
  [f]
  (fn [table]
    (let [preparation (remove nil? (if (sequential? f) f [f]))]
      [nil (update-in table [:prepare] concat preparation)])))

(defn insert-returning [db table rows]
  (sql/insert table []
    (sql/values rows)
    (apply sql/returning (remove #(= true (:hidden? %1)) (columns table)))
    (prepare #(io/encode-row db table %1))
    (prepare (:prepare table))))

(defprotocol Adapter
  (-insert [db table rows]))

(defrecord MySQL [host port user password]
  Adapter
  (-insert [db table row]))

(defrecord PostgreSQL [host port user password]
  Adapter
  (-insert [db table rows]
    (insert-returning db table rows)))

(defn mysql
  "Returns a MySQL adapter from `spec`."
  [spec]
  (merge (->MySQL "localhost" 3306 (util/current-user) nil)
         (util/parse-db-url spec)))

(defn postgresql
  "Returns a PostgreSQL adapter from `spec`."
  [spec]
  (merge (->PostgreSQL "localhost" 5432 (util/current-user) nil)
         (util/parse-db-url spec)))
