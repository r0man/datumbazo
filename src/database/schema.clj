(ns database.schema
  (:require [clojure.java.jdbc :as jdbc]
            [database.protocol :refer [as-identifier]]))

(defrecord Schema [name tables]
  database.protocol.IIdentifier
  (as-identifier [k]
    (jdbc/as-identifier name)))

(defn make-schema
  "Make a new database schema map."
  [name & tables]
  (Schema.
   (keyword name)
   (zipmap (map (comp keyword :name) tables) tables)))

(defn create-schema
  "Create the database `schema`."
  [schema]
  (-> (format "CREATE SCHEMA %s" (as-identifier schema))
      (jdbc/do-commands)))
