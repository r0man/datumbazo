(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.columns
        database.tables))

(defmulti add-column (fn [table column] (:type column)))

(defn create-table
  "Create the database table."
  [table]
  (jdbc/transaction
   (->> (remove #(not (nil? (:add-fn %))) (:columns table))
        (map column-spec)
        (apply jdbc/create-table (table-identifier table)))
   (doseq [column (filter #(:add-fn %) (:columns table))]
     ((:add-fn column) table column))
   table))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (if-let [table table]
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (jdbc/as-identifier (table-identifier table))
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (->> (map #(apply make-column %1) columns)
       (make-table :name name :columns)
       (register-table)))
