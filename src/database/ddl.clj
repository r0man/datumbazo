(ns database.ddl
  (:require [clojure.java.jdbc :as jdbc]
            [database.protocol :refer [as-identifier]]
            [database.columns :refer [column-spec]]
            [database.meta :refer [with-table]]))

;; (defmulti add-column
;;   "Add the `column` to the database `table`."
;;   (fn [table column] (:type column)))

;; (defmethod add-column :default [table column]
;;   (with-ensure-column [table [column column]]
;;     (jdbc/do-commands (add-column-statement (:table column) column))
;;     column))

;; (defn drop-column
;;   "Drop `column` from `table`."
;;   [table column]
;;   (with-ensure-column [table [column column]]
;;     (jdbc/do-commands (drop-column-statement (:table column) column))))

(defn create-schema
  "Drop the database `schema`."
  [schema]
  (-> (format "CREATE SCHEMA %s" (as-identifier schema))
      (jdbc/do-commands)))

(defn create-table
  "Create the database `table`."
  [table]
  (with-table [table table]
    (jdbc/transaction
     (->> (filter :native? (vals (:columns table)))
          (map column-spec)
          (apply jdbc/create-table (:table-name table)))
     ;; (doseq [column (remove :native? (vals (:columns table)))]
     ;;   (add-column table column))
     table)))

(defn drop-schema
  "Drop the database `schema`."
  [schema]
  (-> (format "DROP SCHEMA %s" (as-identifier schema))
      (jdbc/do-commands)))

(defn drop-table
  "Drop the database `table`."
  [table & {:keys [if-exists cascade restrict]}]
  (jdbc/do-commands
   (str "DROP TABLE " (if if-exists "IF EXISTS ")
        (as-identifier table)
        (if cascade " CASCADE")
        (if restrict " RESTRICT"))))
