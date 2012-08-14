(ns database.schema
  (:refer-clojure :exclude [drop])
  (:require [clojure.java.jdbc :as jdbc]
            [database.protocol :refer [as-identifier]])
  (:import database.protocol.Nameable))

(defrecord Schema [name]
  Nameable
  (as-identifier [schema]
    (jdbc/as-identifier (:name schema))))

(defn drop-schema
  "Drop the database `schema`."
  [schema]
  (-> (format "DROP SCHEMA %s" (as-identifier schema))
      (jdbc/do-commands)))

(defn create-schema
  "Drop the database `schema`."
  [schema]
  (-> (format "CREATE SCHEMA %s" (as-identifier schema))
      (jdbc/do-commands)))

(defn make-schema
  "Make a new database schema map."
  [name]
  (assert (keyword name) (str "Invalid schema name: " (prn-str name)))
  (Schema. (keyword name)))
