(ns datumbazo.fixtures
  (:gen-class)
  (:refer-clojure :exclude [distinct group-by replace])
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file make-parents resource writer]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? join replace split]]
            [clojure.tools.logging :refer [infof]]
            [commandline.core :refer [print-help with-commandline]]
            [datumbazo.connection :refer [connection-spec]]
            [datumbazo.core :refer :all :exclude [join]]
            [datumbazo.core :refer [select from run1]]
            [datumbazo.io :refer [encode-rows decode-row]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [edn-file-seq path-split path-replace]]
            [geo.postgis :as geo]
            [inflections.core :refer [hyphenate underscore]]
            [sqlingvo.expr :refer [parse-table]]))

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
      (run1 (select db [`(setval ~(sql-name db (serial-seq column))
                                 ~(select db [`(max ~(:name column))]
                                    (from table)))])))))

(defn- find-column-keys [db table]
  (let [table (parse-table table)]
    (->> (meta/columns db :schema (:schema table) :table (:name table))
         (map (comp #(sql-keyword db %) :column-name)))))

(defn read-fixture
  "Read the fixtures form `filename` and insert them into the database `table`."
  [db table filename & {:keys [entities batch-size]}]
  (infof "Loading fixtures for table %s from %s." (sql-name db table) filename)
  (let [batch-size (or batch-size 1000)
        columns (find-column-keys db table)
        rows (reduce
              (fn [result rows]
                (concat result
                        ;; TDOD: Find insert columns and do not rely on first row.
                        (run (insert db table columns
                               (values (encode-rows db table rows))
                               (returning *)))))
              [] (partition batch-size batch-size nil (slurp-rows filename)))
        result (assoc {:table table :file filename} :records rows)]
    (reset-serials db table)
    result))

(defn write-fixture
  "Write the rows of the database `table` to `filename`."
  [db table filename & {:keys [entities identifiers]}]
  (make-parents filename)
  (with-open [writer (writer filename)]
    (let [rows (run (select db [*] (from table)))]
      (clojure.pprint/pprint rows writer)
      {:file filename :table table :records (count rows)})))

(defn deferred-constraints [db]
  (jdbc/execute! db ["SET CONSTRAINTS ALL DEFERRED"]))

(defn enable-triggers
  "Enable triggers on the database `table`."
  [db table & {:keys [entities]}]
  (jdbc/execute! db [(str "ALTER TABLE " (sql-name db table) " ENABLE TRIGGER ALL")]))

(defn disable-triggers
  "Disable triggers on the database `table`."
  [db table & {:keys [entities]}]
  (jdbc/execute! db [(str "ALTER TABLE " (sql-name db table) " DISABLE TRIGGER ALL")]))

(defn delete-fixtures [db tables]
  (infof "Deleting fixtures from database.")
  (doseq [table tables]
    (run (truncate db [table] (cascade true)))))

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
    (jdbc/with-db-transaction
      [db db]
      (apply deferred-constraints db opts)
      (doall (map #(apply disable-triggers db %1 opts) (map :table fixtures)))
      (let [fixtures (->> fixtures
                          (map #(apply read-fixture db (:table %1) (:file %1) opts))
                          (doall))]
        (doall (map #(apply enable-triggers db %1 opts) (map :table fixtures)))
        fixtures))))

(defn tables [directory]
  (map :table (fixture-seq directory)))

(defn show-help []
  (print-help "fixtures DB-URL DIRECTORY [delete|dump|load|]")
  (System/exit 0))

(defn -main [& args]
  (with-commandline [[db-url directory command] args]
    [[h help "Print this help."]]
    (when (or help (nil? db-url) (nil? directory))
      (show-help))
    (let [db (connection-spec db-url)
          tables (tables directory)]
      (case (keyword command)
        :delete (delete-fixtures db tables)
        :dump (dump-fixtures db tables)
        :load (load-fixtures db directory)
        (show-help))
      nil)))
