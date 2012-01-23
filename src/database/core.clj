(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (join)]
        [inflections.core :only (camelize singular plural)]
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
  (with-ensure-table table
    (let [columns (key-columns table record)]
      (cons (join " OR " (map #(str (column-identifier %1) " = ?") columns))
            (map #(get (serialize-column %1 record) (column-keyword %1)) columns)))))

(defn delete-all
  "Delete all rows from table."
  [table] (first (jdbc/do-commands (str "DELETE FROM " (table-identifier table)))))

(defn delete-where
  "Delete all rows from table matching the where clause."
  [table where-clause] (first (jdbc/delete-rows (table-identifier table) where-clause)))

(defn delete-record
  "Delete the record from the database table."
  [table record]
  (if (not (empty? record))
    (with-ensure-table table
      (let [where-clause (where-clause table record)]
        (assert (not (empty? where-clause)) "Can't build where clause to delete record.")
        (jdbc/transaction
         (assert (= 1 (delete-where table where-clause)))
         record)))))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (with-ensure-table table
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (table-identifier table)
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

(defn insert-record
  "Insert a record into the database table."
  [table record]
  (if (not (empty? record))
    (with-ensure-table table
      (->> (serialize-row table record)
           (jdbc/insert-record (table-identifier table))
           (deserialize-row table)))))

(defn update-record
  "Update a record into the database table."
  [table record & attributes]
  (if (not (empty? record))
    (with-ensure-table table
      (let [record (if attributes (apply assoc record attributes) record)]
        (->> (serialize-row table record)
             (jdbc/update-values (table-identifier table) (where-clause table record)))
        record))))

(defn select-by-column
  "Find a record in the database table by id."
  [table column value]
  (with-ensure-table table
    (let [column (or (column? column) (first (select-columns table [column])))]
      (assert (column? column))
      (jdbc/with-query-results rows
        [(format
          "SELECT * FROM %s WHERE %s = ?"
          (table-identifier table)
          (column-identifier column)) (if value ((or (:serialize column) identity) value))]
        (doall (map (partial deserialize-row table) rows))))))

(defn table
  "Lookup table in *tables* by name."
  [table] (find-table table))

(defn- define-crud
  [table]
  (let [entity# (singular (table-symbol table))]
    `(do
       (defn ~(symbol (str "delete-" entity#))
         ~(format "Delete the %s from the database." entity#)
         [~'record] (delete-record ~(table-keyword table) ~'record))
       (defn ~(symbol (str "insert-" entity#))
         ~(format "Insert the %s into the database." entity#)
         [~'record] (insert-record ~(table-keyword table) ~'record))
       (defn ~(symbol (str "update-" entity#))
         ~(format "Update the %s in the database." entity#)
         [~'record & ~'options] (apply update-record ~(table-keyword table) ~'record ~'options)))))

(defn- define-finder
  [table column]
  (let [name ((if (unique-column? column) singular plural) (singular (table-symbol table)))]
    `(defn ~(symbol (format "find-%s-by-%s" name (column-symbol column)))
       ~(format "Find the %s by the %s column in the database." name (column-keyword column))
       [~'value]
       (~(if (unique-column? column) first identity)
        (select-by-column (find-table ~(table-keyword table)) ~(:name column) ~'value)))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (let [name# name columns# columns table# (make-table (keyword name#) columns#)]
    `(do
       (register-table (make-table ~(keyword name#) ~columns#))
       ~(define-serialization table#)
       ~(define-crud table#)
       ~@(map #(define-finder table# %1) (:columns table#)))))
