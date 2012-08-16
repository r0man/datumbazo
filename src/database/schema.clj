(ns database.schema
  (:refer-clojure :exclude [drop])
  (:require [clojure.java.jdbc :as jdbc]
            [database.protocol :refer [as-identifier as-keyword]]
            [database.util :refer [lookup register]]
            [inflections.core :refer [hyphenize]])
  (:import database.protocol.Nameable))

(defonce ^:dynamic *schemas*
  (atom {}))

(defrecord Schema [name]
  Nameable
  (as-keyword [schema]
    (jdbc/as-keyword (:name schema)))
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
  (Schema. (keyword (hyphenize name))))

(defn read-schemas
  "Read the database schema from the current database connection."
  [] (->> (.getSchemas (.getMetaData (jdbc/connection)))
          (resultset-seq)
          (map #(make-schema (:table_schem %1)))))

(defn schema-key
  "Returns the lookup key for `schema` in *schema*"
  [schema] [(as-keyword schema)])

(defn register-schema
  "Register the database schema in *schemas*."
  [schema] (register *schemas* (schema-key schema) schema))

(defn lookup-schema
  "Lookup `schema` in *schemas*."
  [schema] (lookup *schemas* (schema-key schema)))

(defn load-schemas
  "Load the database schema from the current database connection."
  [] (doall (map register-schema (read-schemas))))

(register-schema (make-schema :public))

(comment
  (database.connection/with-database :bs-database
    (load-schemas)))