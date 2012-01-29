(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only (join)]
        [inflections.core :only (camelize singular plural)]
        [korma.core :exclude (join table)]
        [korma.sql.fns :only (pred-or)]
        database.columns
        database.tables
        database.serialization
        database.pagination))

(defn table->entity [table]
  (with-ensure-table table
    (-> (create-entity (table-identifier table))
        (transform (partial deserialize-record table)))))

(defn unique-key-clause
  "Returns the SQL where clause for record."
  [table record]
  (with-ensure-table table
    (let [columns (key-columns table record)]
      (apply pred-or (map #(apply hash-map %1) (seq (select-keys record (map :name columns))))))))

(defn table
  "Lookup table in *tables* by name."
  [table] (find-table table))

;; DDL

(defmulti add-column
  "Add a `column` to the database `table`."
  (fn [table column] (:type column)))

(defmethod add-column :default [table column]
  (jdbc/do-commands (add-column-statement table column))
  column)

(defn create-table
  "Create the database `table`."
  [table]
  (jdbc/transaction
   (->> (filter :native? (vals (:columns table)))
        (map column-spec)
        (apply jdbc/create-table (table-identifier table)))
   (doseq [column (remove :native? (vals (:columns table)))]
     (add-column table column))
   table))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (with-ensure-table table
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (table-identifier table)
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

;; CRUD

(defn delete-all
  "Delete all rows from table."
  [table] (first (jdbc/do-commands (str "DELETE FROM " (table-identifier table)))))

(defn delete-where
  "Delete all rows from table matching the where clause."
  [table where-clause] (first (jdbc/delete-rows (table-identifier table) where-clause)))

(defn delete-record
  "Delete the record from the database table."
  [table record]
  (if-not (empty? record)
    (with-ensure-table table
      (delete (table-identifier table)
              (where (unique-key-clause table record))))))

(defn insert-record
  "Insert the `record` into the database `table`."
  [table record]
  (if-not (empty? record)
    (with-ensure-table table
      (->> (insert (table-identifier table)
                   (values (->> (remove-serial-columns table record)
                                (serialize-record table))))
           (deserialize-record table)))))

(defn update-record
  "Update the `record` in the database `table`."
  [table record]
  (if-not (empty? record)
    (with-ensure-table table
      (->> (update (table-identifier table)
                   (set-fields (serialize-record table record))
                   (where (unique-key-clause table record)))
           (deserialize-record table)))))

(defn reload-record
  "Find the `record` in the database `table`."
  [table record]
  (if-not (empty? record)
    (with-ensure-table table
      (-> (select (table->entity table)
                  (where (unique-key-clause table record)))
          (first)))))

(defn save-record
  "Update or insert the `record` in the database `table`."
  [table record] (or (update-record table record) (insert-record table record)))

(defn select-by-column*
  "Find records in the database `table` by `column` and `value`."
  [table column value]
  (with-ensure-column table column
    (-> (select* (table->entity table))
        (where {(column-keyword column) (serialize-column column value)}))))

(defn select-by-column
  "Find records in the database `table` by `column` and `value`."
  [table column value & {:keys [page per-page]}]
  (if (or page per-page)
    (paginate*
     (select-by-column* table column value)
     :page page :per-page per-page)
    (exec (select-by-column* table column value))))

(defn- define-crud
  [table]
  (let [entity# (singular (table-symbol table))]
    `(do
       (defn ~(symbol (format "make-%s" entity#)) [& {:as ~'attributes}]
         ~'attributes)
       (defn ~(symbol (str "delete-" entity#))
         ~(format "Delete the %s from the database." entity#)
         [~'record] (delete-record ~(table-keyword table) ~'record))
       (defn ~(symbol (str "insert-" entity#))
         ~(format "Insert the %s into the database." entity#)
         [~'record] (insert-record ~(table-keyword table) ~'record))
       (defn ~(symbol (str "update-" entity#))
         ~(format "Update the %s in the database." entity#)
         [~'record & ~'options] (apply update-record ~(table-keyword table) ~'record ~'options))
       (defn ~(symbol (str "save-" entity#))
         ~(format "Save the %s in the database." entity#)
         [~'record & ~'options] (apply save-record ~(table-keyword table) ~'record ~'options)))))

(defn- define-finder
  [table column]
  (if (unique-column? column)
    `(defn ~(symbol (format "%s-by-%s" (singular (table-symbol table)) (column-symbol column)))
       ~(format "Find the first %s by the %s column in the database." (singular (table-symbol table)) (column-symbol column))
       [~'value] (first (select-by-column ~(table-keyword table) ~(:name column) ~'value)))
    `(defn ~(symbol (format "%s-by-%s" (table-symbol table) (column-symbol column)))
       ~(format "Find %s by the %s column in the database." (table-symbol table) (column-symbol column))
       [~'value & ~'options] (apply select-by-column ~(table-keyword table) ~(:name column) ~'value ~'options))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (let [name# name columns# columns table# (make-table (keyword name#) columns#)]
    `(do
       (register-table (make-table ~(keyword name#) ~columns#))
       ~(define-serialization table#)
       ~(define-crud table#)
       ~@(map #(define-finder table# %1) (vals (:columns table#))))))