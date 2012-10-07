(ns database.fixtures
  (:refer-clojure :exclude [replace])
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file resource writer]]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.string :refer [join split replace]]
            [database.util :refer [absolute-path clojure-file-seq file-split]]))

(def ^:dynamic *readers*
  {'inst read-instant-timestamp})

(defn- resolve-table
  "Resolve the table name form `directory` and `filename`."
  [directory filename]
  (let [directory (absolute-path directory)
        filename (absolute-path filename)]
    (-> (join "." (file-split (replace filename (str directory "/") "")))
        (replace #"(?i)\.cljs?$" "")
        (keyword))))

(defn find-fixtures
  "Returns tree a seq of fixtures in `directory`."
  [directory]
  (for [file (clojure-file-seq directory)]
    {:file file :table (resolve-table directory file)}))

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
      (pprint rows writer)
      {:file filename :table table :records (count rows)})))

(defn load-fixtures
  "Load all database fixtures from `directory`."
  [directory]
  (jdbc/transaction
   (->> (find-fixtures directory)
        (map #(read-fixture (:table %1) (:file %1)))
        (doall))))
