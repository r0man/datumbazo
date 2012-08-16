(ns database.ddl
  (:require [clojure.java.jdbc :as jdbc]
            [database.protocol :refer [as-identifier]]))

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
