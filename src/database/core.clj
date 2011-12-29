(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use [database.columns :only (column-spec)]
        [database.tables :only (table-name)]))

(defn create-table
  "Create the database table."
  [table]
  (jdbc/transaction
   (->> (remove #(not (nil? (:add-fn %))) (:columns table))
        (map column-spec)
        (apply jdbc/create-table (table-name table)))
   (doseq [column (filter #(:add-fn %) (:columns table))]
     ((:add-fn column) table column))
   table))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (if-let [table table]
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (jdbc/as-identifier (table-name table))
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))