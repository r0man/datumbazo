(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use database.columns
        database.tables))

(defmulti add-column
  "Add column to the database table."
  (fn [table column] (:type column)))

(defmethod add-column :default [table column]
  (jdbc/do-commands (add-column-statement table column))
  column)

(defn create-table
  "Create the database table."
  [table]
  (jdbc/transaction
   (->> (filter :native? (:columns table))
        (map column-spec)
        (apply jdbc/create-table (table-identifier table)))
   (doseq [column (remove :native? (:columns table))]
     (add-column table column))
   table))

(defn delete-rows
  "Delete rows from the database table. If the optional where clause
  is given, only those rows matching the clause will be deleted."
  [table & [where]]
  (if where
    (jdbc/delete-rows (table-identifier table) where)
    (jdbc/do-commands (str "DELETE FROM " (table-identifier table)))))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (if-let [table table]
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (table-identifier table)
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (->> (map #(apply make-column %1) columns)
       (make-table :name name :columns)
       (register-table)))
