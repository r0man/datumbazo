(ns datumbazo.fixtures
  (:gen-class)
  (:require [clojure.instant :refer [read-instant-timestamp]]
            [clojure.java.io :refer [file make-parents resource writer]]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [commandline.core :refer [print-help with-commandline]]
            [datumbazo.core :as sql]
            [datumbazo.driver.core :as driver]
            [datumbazo.io :refer [encode-rows decode-row]]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [edn-file-seq path-split path-replace]]
            [geo.postgis :as geo]
            [inflections.core :refer [hyphenate underscore]]
            [sqlingvo.expr :refer [parse-table]]
            [sqlingvo.util :refer [sql-name sql-keyword sql-quote]]))

(def ^:dynamic *readers*
  (assoc geo/*readers* 'inst read-instant-timestamp))

(defn- resolve-table
  "Resolve the table name from `directory` and `filename`."
  [directory filename]
  (-> (str/join "." (path-split (path-replace filename directory)))
      (str/replace #"(?i)\.edn$" "")
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

(defn serial-sequence
  "Return the serial sequence for `column` in `db`."
  [db column]
  (sql/select db [`(pg_get_serial_sequence
                    ~(if (:schema column)
                       (str (sql-name db (:schema column)) "."
                            (sql-name db (:table column)))
                       (sql-name db (:table column)))
                    ~(sql-name db (:name column)))]))

(defn- select-table-columns
  [db table]
  (meta/columns
   db {:schema (or (:schema table)
                   (some-> (meta/current-schema db) keyword))
       :table (:name table)}))

(defn reset-serials
  "Reset the serial counters of all columns in `table`."
  [db table]
  (let [table (parse-table table)]
    (doseq [column (select-table-columns db table)
            :when (contains? #{:bigserial :serial} (:type column))]
      @(sql/select db [`(setval
                         ~(serial-sequence db column)
                         ~(sql/select db [`(max ~(:name column))]
                            (sql/from table)))]))))

(defn- find-column-keys [db table]
  (let [table (parse-table table)]
    (->> (select-table-columns db table)
         (map (comp #(sql-keyword db %) :column-name)))))

(defn read-fixture
  "Read the fixtures form `filename` and insert them into the database `table`."
  [db table filename & {:keys [batch-size]}]
  (log/debugf "Loading fixtures for table %s from %s." (sql-name db table) filename)
  (let [batch-size (or batch-size 1000)
        columns (find-column-keys db table)
        rows (reduce
              (fn [result rows]
                (let [stmt (sql/insert db table columns
                             ;; TDOD: Find insert columns and do not rely on first row.
                             (sql/values (encode-rows db table rows))
                             (sql/returning :*))]
                  (log/debugf "Inserting batch of %s rows into %s."
                              (count rows) (sql-name db table))
                  (concat result (when (seq rows) @stmt))))
              [] (partition batch-size batch-size nil (slurp-rows filename)))
        result (assoc {:table table :file filename} :records rows)]
    (reset-serials db table)
    result))

(defn write-fixture
  "Write the rows of the database `table` to `filename`."
  [db table filename & {:keys [identifiers]}]
  (make-parents filename)
  (with-open [writer (writer filename)]
    (let [rows (seq @(sql/select db [:*] (sql/from table)))]
      (pprint rows writer)
      {:file filename :table table :records (count rows)})))

(defn constraints-deferred! [db]
  (let [stmt ["SET CONSTRAINTS ALL DEFERRED"]]
    (log/debugf "Set all constraints to deferred: %s" stmt)
    (driver/-execute (:driver db) db stmt nil)))

(defn constraints-immediate! [db]
  (let [stmt ["SET CONSTRAINTS ALL IMMEDIATE"]]
    (log/debugf "Set all constraints to immediate: %s" stmt)
    (driver/-execute (:driver db) db stmt nil)))

(defn enable-triggers
  "Enable triggers on the database `table`."
  [db table]
  (let [stmt (str "ALTER TABLE " (sql-quote db (sql-name db table)) " ENABLE TRIGGER ALL")]
    (log/debugf "Enable triggers: %s" stmt)
    (driver/-execute (:driver db) db [stmt] nil)))

(defn disable-triggers
  "Disable triggers on the database `table`."
  [db table]
  (let [stmt (str "ALTER TABLE " (sql-quote db (sql-name db table)) " DISABLE TRIGGER ALL")]
    (log/debugf "Disable triggers: %s" stmt)
    (driver/-execute (:driver db) db [stmt] nil)))

(defn delete-fixtures [db tables]
  (log/debugf "Deleting fixtures from database.")
  (sql/with-transaction [db db]
    (doseq [table tables]
      @(sql/truncate db [table] (sql/cascade true)))))

(defn dump-fixtures
  "Write the fixtures for `tables` into `directory`."
  [db directory tables & opts]
  (log/debugf "Dumping fixtures to %s." directory)
  (sql/with-transaction [db db]
    (doseq [table tables
            :let [filename (str (apply file directory (str/split (name table) #"\.")) ".edn")]]
      (apply write-fixture db table filename opts))))

(defn load-fixtures
  "Load all database fixtures from `directory`."
  [db directory & opts]
  (log/debugf "Loading fixtures from %s." directory)
  (let [fixtures (fixture-seq directory)]
    (sql/with-transaction
      [db db]
      (constraints-deferred! db)
      (doseq [table (map :table fixtures)]
        (disable-triggers db table))
      (let [fixtures (->> fixtures
                          (map #(apply read-fixture db (:table %1) (:file %1) opts))
                          (doall))]
        (doall (map #(apply enable-triggers db %1 opts) (map :table fixtures)))
        (constraints-immediate! db)
        fixtures))))

(defn tables [directory]
  (map :table (fixture-seq directory)))

(defn show-help []
  (print-help "fixtures DB-URL DIRECTORY [delete|dump|load|]")
  (System/exit 0))

(defn -main [& args]
  (with-commandline [[opts [db-url directory command]] args]
    [[h help "Print this help."]]
    (when (or (:help opts) (nil? db-url) (nil? directory))
      (show-help))
    (sql/with-db [db db-url]
      (let [tables (tables directory)]
        (case (keyword command)
          :delete (delete-fixtures db tables)
          :dump (dump-fixtures db directory tables)
          :load (load-fixtures db directory)
          (show-help))
        nil))))
