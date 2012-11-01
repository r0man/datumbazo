(ns datumbazo.fixtures
  (:refer-clojure :exclude [replace])
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file resource writer]]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.string :refer [join replace]]
            [datumbazo.util :refer [clojure-file-seq path-split path-replace]]
            [datumbazo.io :refer [decode-row]]))

(def ^:dynamic *fixture-path* "db/fixtures")

(def ^:dynamic *readers*
  {'inst read-instant-timestamp})

(defn- resolve-table
  "Resolve the table name form `directory` and `filename`."
  [directory filename]
  (-> (join "." (path-split (path-replace filename directory)))
      (replace #"(?i)\.cljs?$" "")
      (keyword)))

(defn fixture-seq
  "Returns tree a seq of fixtures in `directory`."
  [directory]
  (for [file (clojure-file-seq directory)]
    {:file file :table (resolve-table directory file)}))

(defn fixture-path
  "Returns the fixture path for `db-name`."
  [db-name] (str (file *fixture-path* db-name)))

(defn fixtures
  "Returns the fixtures for `db-name`."
  [db-name] (fixture-seq (resource (fixture-path db-name))))

(defn slurp-rows
  "Slurp a seq of database rows from `filename`."
  [filename]
  (binding [*data-readers* (merge *data-readers* *readers*)]
    (read-string (slurp filename))))

(defn read-fixture
  "Read the fixtures form `filename` and insert them into the database `table`."
  [table filename]
  (assoc {:table table :file filename}
    :records (->> (slurp-rows filename)
                  (apply jdbc/insert-records table)
                  (count))))

(defn write-fixture
  "Write the rows of the database `table` to `filename`."
  [table filename]
  (with-open [writer (writer filename)]
    (jdbc/with-query-results rows
      [(format "SELECT * FROM %s" (jdbc/as-identifier table))]
      (pprint (map decode-row rows) writer)
      {:file filename :table table :records (count rows)})))

(defn load-fixtures
  "Load all database fixtures from `directory`."
  [directory]
  (jdbc/transaction
   (->> (fixture-seq directory)
        (map #(read-fixture (:table %1) (:file %1)))
        (doall))))
