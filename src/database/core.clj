(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (join)]
        [inflections.core :only (camelize singular)]
        database.columns
        database.tables
        database.serialization))

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

(defn where-clause
  "Returns the SQL where clause for record."
  [table record]
  (let [columns (key-columns table record)]
    (cons
     (join " OR " (map #(str (column-identifier %1) " = ?") columns))
     (map #(get (serialize-column %1 record) (column-keyword %1)) columns))))

(defn delete-rows
  "Delete rows from the database table. If the optional where clause
  is given, only those rows matching the clause will be deleted."
  [table & [where]]
  (if where
    (jdbc/delete-rows (table-identifier table) where)
    (jdbc/do-commands (str "DELETE FROM " (table-identifier table)))))

(defn delete-record
  "Delete the record from the database table."
  [table record]
  (if-let [column (first (filter :primary-key (:columns table)))]
    (jdbc/transaction
     (->> [(str (column-identifier column) " = ?")
           (get record (column-keyword column))]
          (delete-rows table)
          first (= 1) assert)
     record)
    (throw (Exception. "No primary key defined."))))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (if-let [table table]
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (table-identifier table)
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

(defn insert-record
  "Insert a record into the database table."
  [table record]
  (->> (serialize-row table record)
       (jdbc/insert-record (table-identifier table))
       (deserialize-row table)))

(defn- define-row
  "Returns a defrecord form for the table rows."
  [table]
  (let [record# (camelize (singular (table-symbol table)))]
    `(defrecord ~record# [~@(map column-symbol (:columns table))])))

(defn- define-crud
  "Returns a defrecord forms for the crud fns."
  [table]
  (let [entity# (singular (table-symbol table))]
    `(do
       (defn ~(symbol (str "insert-" entity#))
         ~(str "Insert the " entity# " into the database.")
         [~'record] (insert-record (find-table ~(table-keyword table)) ~'record))
       (defn ~(symbol (str "delete-" entity#))
         ~(str "Delete the " entity# " from the database.")
         [~'record] (delete-record (find-table ~(table-keyword table)) ~'record)))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (let [name# name columns# columns table# (make-table (keyword name#) columns#)]
    `(do
       (register-table (make-table ~(keyword name#) ~columns#))
       ~(define-row table#)
       ~(define-deserialization table#)
       ~(define-crud table#))))
