(ns database.core
  (:require [clojure.java.jdbc :as jdbc]
            [korma.sql.engine :as eng])
  (:use [clojure.string :only (blank? upper-case)]
        [clojure.set :only (rename-keys)]
        [inflections.core :only (camelize singular plural)]
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

(immigrate 'korma.core)

(defn- find-by-column-doc [table column]
  (format "Returns a query that finds all %s by the %s column in the database."
          (symbol (table-name table)) (symbol (column-name column))))

(defn- find-by-column-sym [table column]
  (symbol (format "%s-by-%s" (symbol (table-name table)) (symbol (column-name column)))))

(defn- find-first-by-column-doc [table column]
  (format "Find the first %s by the %s column in the database."
          (singular (symbol (table-name table))) (symbol (column-name column))))

(defn- find-first-by-column-sym [table column]
  (symbol (format "%s-by-%s" (singular (symbol (table-name table))) (symbol (column-name column)))))

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

(defn copy-from
  "Copy rows from `filename` into `table`."
  [table filename]
  (let [file (java.io.File. filename)]
    (jdbc/do-commands (format "COPY %s FROM '%s'" table (.getAbsolutePath file)))))

(defn text= [arg-1 arg-2]
  (infix (to-tsvector arg-1) "@@" (plainto-tsquery arg-2)))

(defn entity
  "Returns the Korma entity of `table`."
  [table]
  (with-ensure-table [table table]
    (let [entity (assoc (create-entity (table-name table))
                   ;; :transforms (concat (:transforms table) [(partial deserialize-record table)])
                   ;; :prepares (concat (:prepares table) [(partial serialize-record table)])
                   :transforms (concat [(partial deserialize-record table)] (:transforms table))
                   :prepares (concat [(partial serialize-record table)] (:prepares table))
                   )
          field-keys (keys (apply dissoc (:columns table) (:exclude (:fields table))))]
      (-> (apply fields entity field-keys)
          ;; (assoc :fields field-keys)
          ))))

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
  (with-ensure-table [table table]
    (let [prefix (keyword (gensym (str (name path) "-")))
          aliases (prefix-columns prefix fields "-")
          renaming (zipmap aliases fields)
          qualified (prefix-columns (:name table) fields ".")]
      (-> (update-in query [:ent] #(if (keyword? %1) (entity %1) %1))
          (update-in [:aliases] clojure.set/union (set aliases))
          (update-in [:fields] concat (seq (zipmap qualified aliases)))
          (update-in [:ent :transforms] concat
                     [#(assoc-in (apply dissoc (into {} %1) aliases) [path]
                                 (deserialize-record
                                  table
                                  (-> (select-keys %1 aliases)
                                      (rename-keys renaming))))])))))

(defn all-clause
  [table record]
  (with-ensure-table [table table]
    (let [columns (vals (:columns table))
          record (serialize-record table (select-keys record (map :name columns)))]
      (apply pred-and (map #(apply hash-map %1) (seq record))))))

(defn unique-key-clause
  "Returns the SQL where clause for record."
  [table record]
  (with-ensure-table [table table]
    (let [columns (key-columns table record)
          record (serialize-record table (select-keys record (map :name columns)))]
      (apply pred-or (map #(apply hash-map %1) (seq record))))))

(defn where-clause [table record]
  (with-ensure-table [table table]
    (if (empty? (key-columns table record))
      (all-clause table record)
      (unique-key-clause table record))))

(defn uniqueness-of
  "Validates that the record's columns are unique."
  [table columns & {:as options}]
  (let [message (or (:message options) "has already been taken.")
        columns (if (sequential? columns) columns [columns])]
    (fn [record]
      (with-ensure-table [table table]
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
    (with-ensure-table [table table]
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
  (with-ensure-column [table [column column]]
    (jdbc/do-commands (add-column-statement (:table column) column))
    column))

(defn drop-column
  "Drop `column` from `table`."
  [table column]
  (with-ensure-column [table [column column]]
    (jdbc/do-commands (drop-column-statement (:table column) column))))

(defn create-table
  "Create the database `table`."
  [table]
  (with-ensure-table [table table]
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
  (with-ensure-table [table table]
    (jdbc/do-commands
     (str "DROP TABLE " (if if-exists "IF EXISTS ")
          (table-identifier table)
          (if cascade " CASCADE")
          (if restrict " RESTRICT")))))

(defn reload-record
  "Reload the `record` from the database `table`."
  [table record]
  (if-not (empty? record)
    (with-ensure-table [table table]
      (-> (apply fields
                 (-> (select* (entity table))
                     (where (where-clause table record)))
                 (map :name (default-columns table)))
          exec first))))

(defn validate-record
  "Validate `record`."
  [table record]
  (with-ensure-table [table table]
    (if-let [validation-fn (:validate table)]
      (validation-fn record)
      record)))

;; CRUD

(defn truncate-table
  "Truncate the table."
  [table]
  (with-ensure-table [table table]
    (jdbc/do-commands (str "TRUNCATE " (table-identifier table)))
    table))

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
    (with-ensure-table [table table]
      (delete (table-identifier table)
              (where (where-clause table record)))
      record)))

(defn insert-record
  "Insert the `record` into the database `table`."
  [table record]
  (with-ensure-table [table table]
    (validate-record table record)
    (insert (entity table)
            (values (remove-serial-columns table record)))
    (reload-record table record)))

(defn update-record
  "Update the `record` in the database `table`."
  [table record]
  (with-ensure-table [table table]
    (validate-record table record)
    (update (entity table)
            (set-fields record)
            (where (where-clause table record)))
    (reload-record table record)))

(defn save-record
  "Update or insert the `record` in the database `table`."
  [table record] (or (update-record table record) (insert-record table record)))

(defn define-delete-fn [table]
  (let [singular (singular (symbol (name (:name table))))]
    `(defn ~(symbol (str "delete-" singular))
       ~(format "Reload the %s from the database." singular)
       [~singular]
       (if-not (empty? ~singular)
         (with-ensure-table [~'table ~(:name table)]
           (delete ~(:name table) (where (where-clause ~'table ~singular)))
           ~singular)))))

(defn define-insert-fn [table]
  (let [singular (singular (symbol (name (:name table))))]
    `(defn ~(symbol (str "insert-" singular))
       ~(format "Insert the %s into the database." singular)
       [~singular]
       (with-ensure-table [~'table ~(:name table)]
         (validate-record ~'table ~singular)
         (insert (entity ~'table)
                 (values (remove-serial-columns ~'table ~singular)))
         (~(symbol (str "reload-" singular)) ~singular)))))

(defn define-reload-fn [table]
  (let [singular (singular (symbol (name (:name table))))]
    `(defn ~(symbol (str "reload-" singular))
       ~(format "Reload the %s from the database." singular)
       [~singular]
       (if-not (empty? ~singular)
         (with-ensure-table [~'table ~(:name table)]
           (-> (where (~(symbol (str (name (:name table)) "*")))
                      (where-clause ~'table ~singular))
               exec first))))))

(defn define-update-fn [table]
  (let [singular (singular (symbol (name (:name table))))]
    `(defn ~(symbol (str "update-" singular))
       ~(format "Update the %s in the database." singular)
       [~singular]
       (with-ensure-table [~'table ~(:name table)]
         (validate-record ~'table ~singular)
         (update (entity ~'table)
                 (set-fields ~singular)
                 (where (where-clause ~'table ~singular)))
         (~(symbol (str "reload-" singular)) ~singular)))))

(defn define-save-fn [table]
  (let [singular (singular (symbol (name (:name table))))]
    `(defn ~(symbol (str "save-" singular))
       ~(format "Save the %s in the database." singular)
       [~singular]
       (or (~(symbol (str "update-" singular)) ~singular)
           (~(symbol (str "insert-" singular)) ~singular)))))

(defn define-truncate-fn [table]
  (let [singular (singular (symbol (name (:name table))))]
    `(defn ~(symbol (str "truncate-" singular))
       ~(format "Truncate the %s database table." singular)
       [] (truncate-table ~(:name table)))))

(defmacro defquery [name doc args & body]
  (let [name# name, args# args, query# (symbol (str name# "*"))]
    `(do
       (defn ~query# [~@args#] ~doc
         ~@body)
       (defn ~name [& ~'args] ~doc
         (let [[args# options#] (split-args ~'args)]
           (-> (apply ~query# (apply concat args# (seq (dissoc options# :page :per-page))))
               (paginate* :page (:page options#) :per-page (:per-page options#))))))))

(defmacro defquery [name doc args & body]
  (let [name# name, args# args, query# (symbol (str name# "*"))]
    `(do
       (defn ~query# [~@args# & [{:as ~'options}]] ~doc
         ~@body)
       (defn ~name [~@args# & [{:as ~'options}]] ~doc
         (-> (~query# ~@args# (dissoc ~'options :page :per-page))
             (paginate* :page (:page ~'options) :per-page (:per-page ~'options)))))))

(defn- define-crud
  [table]
  (let [entity# (symbol (singular (table-name table)))]
    `(do
       ~(define-delete-fn table)
       ~(define-reload-fn table)
       ~(define-insert-fn table)
       ~(define-update-fn table)
       ~(define-save-fn table)
       ~(define-truncate-fn table)
       (defn ~(symbol (format "make-%s" entity#)) [& {:as ~'attributes}]
         ~'attributes))))

(defn- define-finder
  [table]
  (let [find-all# (symbol (str (symbol (table-name table)) "*"))]
    `(do
       (defquery ~(symbol (table-name table))
         ~(format "Returns a query that selects all %s in the database." (symbol (table-name table)))
         [] (apply fields (select* (entity ~(keyword (table-name table))))
                   (map :name (default-columns (find-table ~(keyword (table-name table)))))))
       ~@(for [column (vals (:columns table))]
           `(do
              (defquery ~(find-by-column-sym table column)
                ~(find-by-column-doc table column)
                [~'value]
                (with-ensure-column [~(:name table) [~'column ~(:name column)]]
                  (where (~find-all#) (~'= ~(:name column) (serialize-column ~'column ~'value)))))
              (defn ~(find-first-by-column-sym table column)
                ~(find-first-by-column-doc table column)
                [~'value & [{:as ~'options}]] (first (~(find-by-column-sym table column) ~'value ~'options))))))))

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
       ~(define-finder table#)
       ~(define-crud table#))))

;; JOIN

(defn join* [query type table clause & [columns]]
  (with-ensure-table [table table]
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
