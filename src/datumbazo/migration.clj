(ns datumbazo.migration
  (:require [clojure.java.io :refer [make-parents]]
            [inflections.core :refer [parameterize]]
            [datumbazo.core :refer [deftable column]]))

(def root-dir "resources/db/migrations")

(deftable schema-migrations
  "The database table for the schema migration history."
  (column :id :serial :primary-key? true)
  (column :name :text :unique? true)
  (column :created-at :timestamp-with-time-zone :unique? true))

(defn file-exists?
  "Returns true if `f` exists."
  [f] (.exists (java.io.File. (str f))))

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

(defn make-migration [root-dir name & {:keys [time]}]
  {:name name
   :root-dir root-dir
   :deploy (migration-deploy-path root-dir name)
   :revert (migration-revert-path root-dir name)
   :test (migration-test-path root-dir name)})

(defn spit-content [filename content]
  (make-parents filename)
  (spit filename content))

(defn create-migration [migration]
  (spit-content (:deploy migration) "-- DEPLOY COMMANDS")
  (spit-content (:revert migration) "-- REVERT COMMANDS")
  (spit-content (:test migration) "-- TEST COMMANDS"))
