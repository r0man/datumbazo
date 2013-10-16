(ns datumbazo.migration
  (:require [inflections.core :refer [parameterize]]))

(def migration-dir "resources/db/migrations")

(defn migration-path
  "Returns the path to the SQL file that contains the deploy commands."
  [root-dir migration-name type]
  (if (and migration-name type)
    (str root-dir "/" (name type) "/" (parameterize migration-name) ".sql")))

(defn migration-deploy-path
  "Returns the path to the SQL file that contains the deploy commands."
  [root-dir name]
  (migration-path root-dir name :deploy))

(defn migration-revert-path
  "Returns the path to the SQL file that contains the revert commands."
  [root-dir name]
  (migration-path root-dir name :revert))

(defn migration-test-path
  "Returns the path to the SQL file that contains the test commands."
  [root-dir name]
  (migration-path root-dir name :test))
