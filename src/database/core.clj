(ns database.core
  (:require [clojure.java.jdbc :as jdbc]
            [korma.core :as k])
  (:use [clojure.string :only (join)]
        [inflections.core :only (camelize singular plural)]
        [korma.core :exclude (join table)]
        [korma.sql.fns :only (pred-or)]
        database.columns
        database.tables
        database.serialization))

(defn- table->entity [table]
  (-> (create-entity (table-identifier table))
      (transform (partial deserialize-record table))))

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
   (->> (filter :native? (vals (:columns table)))
        (map column-spec)
        (apply jdbc/create-table (table-identifier table)))
   (doseq [column (remove :native? (vals (:columns table)))]
     (add-column table column))
   table))

(defn unique-key-clause
  "Returns the SQL where clause for record."
  [table record]
  (with-ensure-table table
    (let [columns (key-columns table record)]
      (apply pred-or (map #(apply hash-map %1) (seq (select-keys record (map :name columns))))))))

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
      (delete (table-identifier table)
              (where (unique-key-clause table record))))))

(defn drop-table
  "Drop the database table."
  [table & {:keys [if-exists cascade restrict]}]
  (with-ensure-table table
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (table-identifier table)
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

(defn reload-record
  "Find the `record` in the database `table`."
  [table record]
  (if (not (empty? record))
    (with-ensure-table table
      (-> (select (table->entity table)
                  (where (unique-key-clause table record)))
          (first)))))

(defn insert-record
  "Insert the `record` into the database `table`."
  [table record]
  (if (not (empty? record))
    (with-ensure-table table
      (->> (insert (table-identifier table)
                   (values (->> (remove-serial-columns table record)
                                (serialize-record table))))
           (deserialize-record table)))))

(defn update-record
  "Update the `record` in the database `table`."
  [table record]
  (if (not (empty? record))
    (with-ensure-table table
      (->> (update (table-identifier table)
                   (set-fields (serialize-record table record))
                   (where (unique-key-clause table record)))
           (deserialize-record table)))))

(defn select-by-column
  "Find a record in the database table by id."
  [table column value]
  (with-ensure-table table
    (let [column (or (column? column) (first (select-columns table [column])))]
      (assert (column? column))
      (select (table->entity table)
              (where {(column-keyword column)
                      (serialize-column column value)})))))

(defn table
  "Lookup table in *tables* by name."
  [table] (find-table table))

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
         [~'record & ~'options]
         (or (apply ~(symbol (str "update-" entity#)) ~'record ~'options)
             (apply ~(symbol (str "insert-" entity#)) ~'record ~'options))))))

(defn- define-finder
  [table column]
  (let [name ((if (unique-column? column) singular plural) (singular (table-symbol table)))]
    `(defn ~(symbol (format "%s-by-%s" name (column-symbol column)))
       ~(format "Find the %s by the %s column in the database." name (column-keyword column))
       [~'value]
       (~(if (unique-column? column) first identity)
        (select-by-column ~(table-keyword table) ~(:name column) ~'value)))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (let [name# name columns# columns table# (make-table (keyword name#) columns#)]
    `(do
       (register-table (make-table ~(keyword name#) ~columns#))
       ~(define-serialization table#)
       ~(define-crud table#)
       ~@(map #(define-finder table# %1) (vals (:columns table#))))))
