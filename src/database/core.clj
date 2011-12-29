(ns database.core
  (:require [clojure.java.jdbc :as jdbc])
  (:use [database.columns :only (column-spec make-column)]
        [database.tables :only (make-table table-identifier register-table)]))

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

(defmulti add-column (fn [table column] (:type column)))

(defn add-geometry-column [table column code geometry dimension]
  (jdbc/with-query-results _
    ["SELECT AddGeometryColumn(?::text, ?::text, ?::integer, ?::text, ?::integer)"
     (jdbc/as-identifier (:name table))
     (jdbc/as-identifier (:name column))
     code geometry dimension])
  column)

(defmethod add-column :point-2d [table column]
  (add-geometry-column table column 4326 "POINT" 2))

(defmethod add-column :multipolygon-2d [table column]
  (add-geometry-column table column 4326 "MULTIPOLYGON" 2))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns]]
  (->> (map #(apply make-column %1) columns)
       (make-table :name name :columns)
       (register-table)))
