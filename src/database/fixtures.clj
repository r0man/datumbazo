(ns database.fixtures
  (:refer-clojure :exclude [replace])
  (:import java.io.File)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.io :refer [file writer]]
            [clojure.string :refer [join split replace]]
            [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file resource]]
            [clojure.pprint :refer [pprint]]))

(defn- resolve-table [directory filename]
  (let [directory (.getAbsolutePath (file directory))
        filename (.getAbsolutePath (file filename))
        fragments (-> (replace filename (str directory "/") "")
                      (split (re-pattern File/separator)))]
    (keyword (replace (join "." fragments) #".clj" ""))))

(defn clojure-file?
  "Returns true if `path` is a fixture file, otherwise false."
  [path]
  (and (.isFile (file path))
       (re-matches #".*\.cljs?$" (str path))))

(defn find-fixtures
  "Returns tree a seq of fixtures in `directory`."
  [directory]
  (for [file (file-seq (file directory))
        :when (clojure-file? file)]
    {:file file :table (resolve-table directory file)}))

(defn slurp-rows
  "Slurp a seq of database rows from `filename`."
  [filename] (binding [*data-readers* (assoc *data-readers* 'inst read-instant-timestamp)]
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
  (->> (find-fixtures directory)
       (map #(read-fixture (:table %1) (:file %1)))
       (doall)))
