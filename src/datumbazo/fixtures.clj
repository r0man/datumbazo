(ns datumbazo.fixtures
  (:refer-clojure :exclude [distinct group-by replace])
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file make-parents resource writer]]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [clojure.string :refer [blank? join replace split]]
            [datumbazo.core :refer :all :exclude [join]]
            [datumbazo.io :refer [encode-rows decode-row]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [edn-file-seq path-split path-replace]]
            [datumbazo.core :refer [select from run1]]
            [geo.postgis :as geo]
            [inflections.core :refer [hyphenize underscore]]
            [sqlingvo.util :refer [as-identifier as-keyword parse-table]]))

(def ^:dynamic *readers*
  (assoc geo/*readers* 'inst read-instant-timestamp))

(defn- resolve-table
  "Resolve the table name from `directory` and `filename`."
  [directory filename]
  (-> (join "." (path-split (path-replace filename directory)))
      (replace #"(?i)\.edn$" "")
      (keyword)))

(defn fixture-seq
  "Returns tree a seq of fixtures in `directory`."
  [directory]
  (->> (for [file (edn-file-seq directory)]
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

(defn serial-seq
  "Returns the default serial name of column."
  [column]
  (keyword (str (if (and (:schema column)
                         (not (contains? #{:public} (:schema column))))
                  (str (name (:schema column)) "."))
                (name (:table column)) "-" (name (:name column)) "-seq")))

(defn reset-serials
  "Reset the serial counters of all columns in `table`."
  [db table & {:keys [entities]}]
  (let [table (parse-table table)]
    (doseq [column (meta/columns db :schema (or (:schema table) :public) :table (:name table))
            :when (contains? #{:bigserial :serial} (:type column))]
      (run1 db (select [`(setval ~(as-identifier (serial-seq column))
                                 ~(select [`(max ~(:name column))]
                                    (from (as-keyword table))))])
            :entities entities))))

(defn read-fixture
  "Read the fixtures form `filename` and insert them into the database `table`."
  [db table filename & {:keys [entities]}]
  (let [rows (slurp-rows filename)
        rows (if-not (empty? rows)
               (run db (insert table []
                         (values (encode-rows db table rows))
                         (returning *))
                    :entities entities)
               [])
        result (assoc {:table table :file filename} :records rows)]
    (reset-serials db table :entities entities)
    result))

(defn write-fixture
  "Write the rows of the database `table` to `filename`."
  [db table filename & {:keys [entities identifiers]}]
  (make-parents filename)
  (with-open [writer (writer filename)]
    (let [rows (run db (select [*] (from table))
                    :entities entities
                    :identifiers identifiers)]
      (pprint rows writer)
      {:file filename :table table :records (count rows)})))

(defn deferred-constraints [db]
  (jdbc/execute! db ["SET CONSTRAINTS ALL DEFERRED"]))

(defn enable-triggers
  "Enable triggers on the database `table`."
  [db table & {:keys [entities]}]
  (jdbc/with-naming-strategy
    {:entity (or entities as-identifier)}
    (jdbc/execute! db [(str "ALTER TABLE " (as-identifier table) " ENABLE TRIGGER ALL")])))

(defn disable-triggers
  "Disable triggers on the database `table`."
  [db table & {:keys [entities]}]
  (jdbc/with-naming-strategy
    {:entity (or entities as-identifier)}
    (jdbc/execute! db [(str "ALTER TABLE " (as-identifier table) " DISABLE TRIGGER ALL")])))

(defn dump-fixtures
  "Write the fixtures for `tables` into `directory`."
  [db directory tables & opts]
  (doseq [table tables
          :let [filename (str (apply file directory (split (replace (name table) #"-" "_") #"\.")) ".edn")]]
    (apply write-fixture db table filename opts)))

(defn load-fixtures
  "Load all database fixtures from `directory`."
  [db directory & opts]
  (let [fixtures (fixture-seq directory)]
    (jdbc/db-transaction
     [db db]
     (apply deferred-constraints db opts)
     (doall (map #(apply disable-triggers db %1 opts) (map :table fixtures)))
     (let [fixtures (->> fixtures
                         (map #(apply read-fixture db (:table %1) (:file %1) opts))
                         (doall))]
       (doall (map #(apply enable-triggers db %1 opts) (map :table fixtures)))
       fixtures))))
