(ns datumbazo.fixtures
  (:refer-clojure :exclude [replace])
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file make-parents resource writer]]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.string :refer [blank? join replace split]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [clojure-file-seq path-split path-replace]]
            [datumbazo.io :refer [decode-row read-wkt]]
            [datumbazo.core :refer [sql select from run1]]
            [sqlingvo.util :refer [as-keyword parse-table]]))

(def ^:dynamic *readers*
  {'inst read-instant-timestamp
   'wkt read-wkt})

(defn- resolve-table
  "Resolve the table name from `directory` and `filename`."
  [directory filename]
  (-> (join "." (path-split (path-replace filename directory)))
      (replace #"(?i)\.cljs?$" "")
      (keyword)))

(defn fixture-seq
  "Returns tree a seq of fixtures in `directory`."
  [directory]
  (->> (for [file (clojure-file-seq directory)]
         {:file file :table (resolve-table directory file)})
       (sort-by #(:table %1))))

(defn fixture-path
  "Returns the fixture path for `db-name`."
  [db-name] (str (file "db" db-name "fixtures")))

(defn fixtures
  "Returns the fixtures for `db-name`."
  [db-name] (fixture-seq (resource (fixture-path db-name))))

(defn slurp-rows
  "Slurp a seq of database rows from `filename`."
  [filename]
  (binding [*data-readers* (merge *data-readers* *readers*)]
    (read-string (slurp filename))))

(defn serial-seq [column]
  (-> (->> (map column [:schema :table :name])
           (remove #(= :public %1))
           (remove nil?)
           (map name)
           (join "-"))
      (str "-seq")
      (keyword)))

(defn serial-seq
  "Returns the default serial name of column."
  [column]
  (keyword (str (if (and (:schema column)
                         (not (contains? #{:public} (:schema column))))
                  (str (name (:schema column)) "."))
                (name (:table column)) "-" (name (:name column)) "-seq")))

(defn reset-serials
  "Reset the serial counters of all columns in `table`."
  [table]
  (let [table (parse-table table)]
    (doseq [column (meta/columns (jdbc/connection) :schema (or (:schema table) :public) :table (:name table))
            :when (contains? #{:bigserial :serial} (:type column))]
      (run1 (select `(setval ~(jdbc/as-identifier (serial-seq column))
                             ~(-> (select `(max ~(:name column)))
                                  (from (as-keyword table)))))))))

(defn read-fixture
  "Read the fixtures form `filename` and insert them into the database `table`."
  [table filename]
  (let [result (assoc {:table table :file filename}
                 :records (->> (slurp-rows filename)
                               (apply jdbc/insert-records table)
                               (count)))]
    (reset-serials table)
    result))

(defn write-fixture
  "Write the rows of the database `table` to `filename`."
  [table filename]
  (make-parents filename)
  (with-open [writer (writer filename)]
    (jdbc/with-query-results rows
      [(format "SELECT * FROM %s" (jdbc/as-identifier table))]
      (pprint (map decode-row rows) writer)
      {:file filename :table table :records (count rows)})))

(defn deferred-constraints []
  (jdbc/do-commands "SET CONSTRAINTS ALL DEFERRED"))

(defn enable-triggers
  "Enable triggers on the database `table`."
  [table] (jdbc/do-commands (str "ALTER TABLE " (jdbc/as-identifier table) " ENABLE TRIGGER ALL")))

(defn disable-triggers
  "Disable triggers on the database `table`."
  [table] (jdbc/do-commands (str "ALTER TABLE " (jdbc/as-identifier table) " DISABLE TRIGGER ALL")))

(defn dump-fixtures
  "Write the fixtures for `tables` into `directory`."
  [directory tables]
  (doseq [table tables]
    (->> (str (apply file directory (split (replace (name table) #"-" "_") #"\.")) ".clj")
         (write-fixture table))))

(defn load-fixtures
  "Load all database fixtures from `directory`."
  [directory]
  (let [fixtures (fixture-seq directory)]
    (jdbc/transaction
     (deferred-constraints)
     (doall (map disable-triggers (map :table fixtures)))
     (let [fixtures (->> fixtures
                         (map #(read-fixture (:table %1) (:file %1)))
                         (doall))]
       (doall (map enable-triggers (map :table fixtures)))
       fixtures))))
