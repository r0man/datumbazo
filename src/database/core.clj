(ns database.core
  (:require [clojure.java.jdbc :as jdbc]
            [korma.sql.engine :as eng])
  (:use [clojure.string :only (blank? upper-case)]
        [clojure.set :only (rename-keys)]
        [inflections.core :only (camelize singular plural)]
        [korma.core :exclude (join join* table)]
        [korma.sql.engine :only [infix try-prefix]]
        [korma.sql.fns :only (pred-and pred-or)]
        [korma.sql.utils :only (func)]
        database.columns
        database.pagination
        database.registry
        database.serialization
        database.tables
        database.util
        validation.core
        validation.errors))

(defn new-record?
  "Returns true if `record` doesn't have a `id` column, otherwise false."
  [record] (or (nil? (:id record)) (blank? (str (:id record)))))

(defn sql-cast
  "Cast `arg` to `type`."
  [arg type] (func (str "CAST(%s AS " type ")") [(try-prefix arg)]))

(defn to-tsvector
  "Reduce the document text into tsvector."
  [document & [config]]
  (let [config (or config "english")]
    (sqlfn to_tsvector (sql-cast config "regconfig") (sql-cast document "text"))))

(defn plainto-tsquery
  "Produce a tsquery ignoring punctuation."
  [query & [config]]
  (let [config (or config "english")]
    (sqlfn plainto_tsquery (sql-cast config "regconfig") (sql-cast query "text"))))

(defn text= [arg-1 arg-2]
  (infix (to-tsvector arg-1) "@@" (plainto-tsquery arg-2)))

(defn entity
  "Returns the Korma entity of `table`."
  [table]
  (with-ensure-table table
    (let [entity (create-entity (table-name table))
          entity (assoc entity :transforms (:transforms table))
          field-keys (keys (apply dissoc (:columns table) (:exclude (:fields table))))]
      (-> (apply fields entity field-keys)
          (assoc :fields field-keys)
          (transform (partial deserialize-record table))))))

(defn prefix-columns
  "Return the names of `columns` prefixed with `prefix`."
  [prefix columns & [separator]]
  (let [separator (or separator "-")]
    (map #(keyword (str (name prefix) separator (column-name %1))) columns)))

(defn prefix-fields
  [query table prefix fields]
  (let [aliases (prefix-columns prefix fields "-")
        qualified (prefix-columns table fields ".")]
    (-> (update-in query [:aliases] clojure.set/union (set aliases))
        (update-in [:fields] concat (seq (zipmap qualified aliases))))))

(defn shift-fields
  [query table path fields]
  (with-ensure-table table
    (let [prefix (keyword (gensym (str (name path) "-")))
          aliases (prefix-columns prefix fields "-")
          renaming (zipmap aliases fields)
          qualified (prefix-columns (:name table) fields ".")]
      (-> (update-in query [:ent] #(if (keyword? %1) (entity %1) %1))
          (update-in [:aliases] clojure.set/union (set aliases))
          (update-in [:fields] concat (seq (zipmap qualified aliases)))
          (update-in [:ent :transforms] conj
                     #(assoc-in (apply dissoc (into {} %1) aliases) [path]
                                (deserialize-record
                                 table
                                 (-> (select-keys %1 aliases)
                                     (rename-keys renaming)))))))))

(defn all-clause
  [table record]
  (with-ensure-table table
    (let [columns (vals (:columns table))]
      (apply pred-and (map #(apply hash-map %1) (seq (select-keys record (map :name columns))))))))

(defn unique-key-clause
  "Returns the SQL where clause for record."
  [table record]
  (with-ensure-table table
    (let [columns (key-columns table record)]
      (apply pred-or (map #(apply hash-map %1) (seq (select-keys record (map :name columns))))))))

(defn where-clause [table record]
  (with-ensure-table table
    (if (empty? (key-columns table record))
      (all-clause table record)
      (unique-key-clause table record))))

(defn uniqueness-of
  "Validates that the record's columns are unique."
  [table columns & {:as options}]
  (let [message (or (:message options) "has already been taken.")
        columns (if (sequential? columns) columns [columns])]
    (fn [record]
      (with-ensure-table table
        (if (and
             (or (nil? (:if options)) ((:if options) record))
             (or (nil? (:unless options)) (not ((:unless options) record)))
             (not (empty? (select (entity table)
                                  (where (reduce
                                          #(assoc %1
                                             (keyword (column-name %2))
                                             (serialize-column %2 (get record (keyword (column-name %2)))))
                                          {} (select-columns table columns)))))))
          (reduce
           #(add-error-message-on %1 %2 message)
           record columns)
          record)))))

(defn record-exists?
  "Returns true if `record` exists in the database, otherwise false."
  [table record]
  (if-not (empty? record)
    (with-ensure-table table
      (-> (select (entity table) (where (where-clause table record)))
          empty? not))
    false))

(defn table
  "Lookup table in *tables* by name."
  [table] (find-table table))

(defn where-text=
  "Add a full text condition for `term` on `columm` to `query`."
  [query column term]
  (if-not (blank? term)
    (where query {column [text= term]})
    query))

;; DDL

(defmulti add-column
  "Add the `column` to the database `table`."
  (fn [table column] (:type column)))

(defmethod add-column :default [table column]
  (with-ensure-column table column
    (jdbc/do-commands (add-column-statement table column))
    column))

(defn drop-column
  "Drop `column` from `table`."
  [table column]
  (with-ensure-column table column
    (jdbc/do-commands (drop-column-statement table column))))

(defn create-table
  "Create the database `table`."
  [table]
  (with-ensure-table table
    (jdbc/transaction
     (->> (filter :native? (vals (:columns table)))
          (map column-spec)
          (apply jdbc/create-table (table-identifier table)))
     (doseq [column (remove :native? (vals (:columns table)))]
       (add-column table column))
     table)))

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
  "Reload the `record` from the database `table`."
  [table record]
  (if-not (empty? record)
    (with-ensure-table table
      (-> (apply fields
                 (-> (select* (entity table))
                     (where (where-clause table record)))
                 (map :name (default-columns table)))
          exec first))))

(defn validate-record
  "Validate `record`."
  [table record]
  (with-ensure-table table
    (if-let [validation-fn (:validate table)]
      (validation-fn record)
      record)))

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
              (where (where-clause table record)))
      record)))

(defn insert-record
  "Insert the `record` into the database `table`."
  [table record]
  (with-ensure-table table
    (validate-record table record)
    (insert (table-identifier table)
            (values (->> (remove-serial-columns table record)
                         (serialize-record table))))
    (reload-record table record)))

(defn update-record
  "Update the `record` in the database `table`."
  [table record]
  (with-ensure-table table
    (validate-record table record)
    (update (table-identifier table)
            (set-fields (serialize-record table record))
            (where (where-clause table record)))
    (reload-record table record)))

(defn save-record
  "Update or insert the `record` in the database `table`."
  [table record] (or (update-record table record) (insert-record table record)))

(defn select-by-column
  "Returns a query that finds all records in the database `table` by
  `column` and `value`."
  [table column value]
  (with-ensure-column table column
    (-> (select* (entity table))
        (assoc :fields (map :name (default-columns table)))
        (where {(keyword (column-name column))
                (serialize-column column value)}))))

(defn find-by-column
  "Find records in the database `table` by `column` and `value`."
  [table column value & {:keys [page per-page]}]
  (if (or page per-page)
    (paginate*
     (select-by-column table column value)
     :page page :per-page per-page)
    (exec (select-by-column table column value))))

(defn- define-crud
  [table]
  (let [entity# (symbol (singular (table-name table)))]
    `(do
       (defn ~(symbol (format "make-%s" entity#)) [& {:as ~'attributes}]
         ~'attributes)
       (defn ~(symbol (str "delete-" entity#))
         ~(format "Delete the %s from the database." entity#)
         [~'record] (delete-record ~(keyword (table-name table)) ~'record))
       (defn ~(symbol (str "insert-" entity#))
         ~(format "Insert the %s into the database." entity#)
         [~'record] (insert-record ~(keyword (table-name table)) ~'record))
       (defn ~(symbol (str "update-" entity#))
         ~(format "Update the %s in the database." entity#)
         [~'record & ~'options] (apply update-record ~(keyword (table-name table)) ~'record ~'options))
       (defn ~(symbol (str "save-" entity#))
         ~(format "Save the %s in the database." entity#)
         [~'record & ~'options] (apply save-record ~(keyword (table-name table)) ~'record ~'options)))))

(defn- define-finder
  [table]
  (let [finder# (symbol (str (symbol (table-name table)) "*"))]
    `(do
       (defn ~finder#
         ~(format "Returns a query that selects all %s in the database." (symbol (table-name table)))
         [] (apply fields (select* (entity ~(keyword (table-name table))))
                   (map :name (default-columns (find-table ~(keyword (table-name table)))))))
       (defn ~(symbol (str (symbol (table-name table)) ""))
         ~(format "Find all %s in the database." (symbol (table-name table)))
         [& {:keys [~'page ~'per-page]}]
         (paginate* (~finder#) :page ~'page :per-page ~'per-page))
       ~@(for [column (vals (:columns table))]
           `(do
              (defn ~(symbol (format "%s-by-%s" (symbol (table-name table)) (symbol (column-name column))))
                ~(format "Find all %s by the %s column in the database." (symbol (table-name table)) (symbol (column-name column)))
                [~'value & ~'options] (apply find-by-column ~(keyword (table-name table)) ~(:name column) ~'value ~'options))
              (defn ~(symbol (format "%s-by-%s*" (symbol (table-name table)) (symbol (column-name column))))
                ~(format "Returns a query that finds all %s by the %s column in the database." (symbol (table-name table)) (symbol (column-name column)))
                [~'value] (select-by-column ~(keyword (table-name table)) ~(:name column) ~'value))
              (defn ~(symbol (format "%s-by-%s" (singular (symbol (table-name table))) (symbol (column-name column))))
                ~(format "Find the first %s by the %s column in the database." (singular (symbol (table-name table))) (symbol (column-name column)))
                [~'value] (first (find-by-column ~(keyword (table-name table)) ~(:name column) ~'value))))))))

(defmacro deftable
  "Define and register a database table and it's columns."
  [name & [columns & options]]
  (let [name# name
        columns# columns
        options# options
        table# (apply make-table (keyword name#) columns# options#)]
    `(do
       (register-table (make-table ~(keyword name#) ~columns# ~@options#))
       (def ~(symbol (str name# "-entity")) (entity ~(keyword name#)))
       ~(define-serialization table#)
       ~(define-crud table#)
       ~(define-finder table#))))

(defmacro defquery [name doc args & body]
  (let [name# name args# args
        query# (symbol (str name# "*"))]
    `(do
       (defn ~query# [~@args#] ~doc
         ~@body)
       (defn ~name [& ~'args] ~doc
         (let [[args# options#] (split-args ~'args)]
           (-> (apply ~query# (apply concat args# (seq (dissoc options# :page :per-page))))
               (paginate* :page (:page options#) :per-page (:per-page options#))))))))

;; JOIN

(defn join* [query type table clause & [columns]]
  (with-ensure-table table
    (let [columns (if-not (empty? columns)
                    (vals (select-keys (:columns table) columns))
                    (default-columns table))]
      (-> (update-in query [:joins] conj [type (:name table) clause])
          (shift-fields (:name table) (singular (:name table)) (map :name columns))))))

(defmacro join
  "Add a join clause to a select query, specifying the table name to join and the predicate
  to join on.

  (join query addresses)
  (join query addresses (= :addres.users_id :users.id))
  (join query :right addresses (= :address.users_id :users.id))"
  ([query ent]
     `(let [q# ~query
            e# ~ent
            rel# (get-rel (:ent q#) e#)]
        (join* q# :left e# (sfns/pred-= (:pk rel#) (:fk rel#)))))
  ([query table clause]
     `(join* ~query :left ~table (eng/pred-map ~(eng/parse-where clause))))
  ([query type table clause]
     `(join* ~query ~type ~table (eng/pred-map ~(eng/parse-where clause))))
  ([query type table clause columns]
     `(join* ~query ~type ~table (eng/pred-map ~(eng/parse-where clause)) ~columns)))
