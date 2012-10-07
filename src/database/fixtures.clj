(ns database.fixtures
  (:refer-clojure :exclude [replace])
  (:import java.io.File)
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :refer [file writer]]
            [clojure.string :refer [join split replace]]
            [clojure.java.io :refer [file resource]]
            [clojure.pprint :refer [pprint]]))

(defn- resolve-table [directory filename]
  (let [directory (.getAbsolutePath (file directory))
        filename (.getAbsolutePath (file filename))
        fragments (-> (replace filename (str directory "/") "")
                      (split (re-pattern File/separator)))]
    (replace (join "." fragments) #".clj" "")))

(defn fixture-file?
  "Returns true if `path` is a fixture file, otherwise false."
  [path]
  (and (.isFile (file path))
       (re-matches #".*\.cljs?$" (str path))))

(defn fixtures-in-directory
  "Returns tree a seq of fixture files in `directory`."
  [directory] (filter fixture-file? (file-seq (file directory))))

(defn fixtures-on-classpath
  "Returns tree a seq of fixture files in `directory` on the classpath."
  [directory] (filter fixture-file? (file-seq (file (resource directory)))))

(defn read-table
  "Read `filename` as a seq of rows and insert them into the database `table`."
  [table filename]
  (binding [*data-readers* (assoc *data-readers* 'inst read-instant-timestamp)]
    (->> (read-string (slurp filename))
         (apply jdbc/insert-records table))))

(defn write-table
  "Write the rows of the database `table` to `filename`."
  [table filename]
  (with-open [writer (writer filename)]
    (jdbc/with-query-results rows
      [(format "SELECT * FROM %s" (jdbc/as-identifier table))]
      (pprint rows writer))))

(defn write-fixtures [directory]
  )

(defn read-fixtures
  "Read all fixtures from `directory` and load them into the database."
  [directory]
  (doseq [file (fixture-seq (file directory))
          :let [table (resolve-table directory file)]]
    (read-table table file)))

;; (database.connection/with-database "jdbc:postgresql://localhost/burningswell_development"
;;   (read-fixtures "fixtures"))

;; (database.connection/with-database "jdbc:postgresql://localhost/burningswell_development"
;;   (write-table :continents "fixtures/continents.clj"))

;; (filter #(re-matches #".*.clj" (str %1)) (classpath))

;; (resource "db/fixtures/test-db/continents.clj")
;; (file-seq (file (resource "db/fixtures/test-db")))
