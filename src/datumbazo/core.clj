(ns datumbazo.core
  (:refer-clojure :exclude [group-by])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.connection :as connection]
            [datumbazo.io :as io]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [immigrate parse-table]]
            [inflections.core :refer [hyphenize singular]]
            [inflections.util :refer [parse-integer]]
            [pallet.thread-expr :refer [when->]]))

(immigrate 'sqlingvo.core)

(def ^:dynamic *per-page* 25)

(defn as-identifier [obj]
  (cond
   (keyword? obj)
   (jdbc/as-identifier obj)
   (string? obj)
   obj
   (map? obj)
   (->> [(:schema obj) (:table obj) (:name obj)]
        (map jdbc/as-identifier)
        (remove str/blank?)
        (str/join "."))))

(defn as-keyword [obj]
  (cond
   (keyword? obj)
   obj
   (string? obj)
   (keyword (hyphenize obj))
   (map? obj)
   (->> [(:schema obj) (:table obj) (:name obj)]
        (map jdbc/as-identifier)
        (remove str/blank?)
        (str/join ".")
        (keyword))))

(defmacro run
  "Run `stmts` against the current clojure.java.jdbc database
  connection ans return all rows."
  [& stmts] `(doall (map io/decode-row (sqlingvo.core/run ~@stmts))))

(defmacro run1
  "Run `stmts` against the current clojure.java.jdbc database
  connection and return the first row."
  [& stmts] `(io/decode-row (sqlingvo.core/run1 ~@stmts)))

(defn insert
  "Insert rows into the database `table`."
  [table what]
  (if (sequential? what)
    (-> (sqlingvo.core/insert table (io/encode-rows table what))
        (returning *))
    (-> (sqlingvo.core/insert table what)
        (returning *))))

(defn update
  "Update `row` to the database `table`."
  [table row]
  (if (map? row)
    (let [table (parse-table table)
          pks (meta/primary-keys (jdbc/connection) :schema (:schema table) :table (:name table))
          pk-keys (map :name pks)
          pk-vals (map row pk-keys)]
      (-> (sqlingvo.core/update (as-keyword table) (io/encode-row (as-keyword table) row))
          (where (cons 'and (map #(list '= %1 %2) pk-keys pk-vals)))
          (returning *)))
    (sqlingvo.core/update (as-keyword table) row)))

(defn count-all
  "Count all rows in the database `table`."
  [table] (:count (run1 (select '(count *)) (from table))))

(defn make-table
  "Make a database table."
  [name & {:as options}]
  (assoc options
    :name (keyword name)))

(defn make-column
  "Make a database column."
  [name type & {:as options}]
  (assoc options
    :name (keyword name)
    :type type))

(defn column
  "Add a database column to `table`."
  [table name type & options]
  (let [column (apply make-column name type options)]
    (-> (update-in table [:columns] #(concat %1 [(:name column)]))
        (assoc-in [:column (:name column)] column))))

(defn paginate
  "Add LIMIT and OFFSET clauses to `query` according to
  `page` (default: 1) and `per-page` (default: `*per-page*`)."
  [query & {:keys [page per-page]}]
  (let [page (parse-integer (or page 1))
        per-page (parse-integer (or per-page *per-page*))]
    (-> (limit query per-page)
        (offset (* (dec page) (or per-page *per-page*))))))

(defn schema
  "Assoc `schema` under the :schema key to `table`."
  [table schema] (assoc table :schema schema))

(defn table
  "Assoc `name` under the :name key to `table`."
  [table name] (assoc table :name name))

(defmacro defquery [name doc args body & [map-fn]]
  (let [query-sym (symbol (str name "*"))]
    `(do (defn ~query-sym ~doc ~args
           ~body)
         (defn ~name ~doc ~args
           (->> (run ~body)
                (map (or ~map-fn identity)))))))

(defmacro defquery1 [name doc args body & [map-fn]]
  (let [query-sym (symbol (str name "*"))]
    `(do (defn ~query-sym ~doc ~args
           ~body)
         (defn ~name ~doc ~args
           (->> (run1 ~body)
                ((or ~map-fn identity)))))))

(defmacro deftable
  "Define a database table."
  [table-name doc & body]
  (let [table# (eval `(-> (make-table ~(keyword table-name) :doc ~doc) ~@body))
        symbol# (symbol (str table-name "-table"))]
    `(do (def ~symbol#
           (-> (make-table ~(keyword table-name) :doc ~doc)
               ~@body))

         (defn ~(symbol (str "drop-" table-name))
           ~(format "Drop the %s database table." table-name)
           [& ~'opts] (:count  (run1 (apply drop-table ~(as-keyword table#) ~'opts))))

         (defn ~(symbol (str "delete-" table-name))
           ~(format "Delete all rows in the %s database table." table-name)
           [& ~'opts] (:count (run1 (delete ~(as-keyword table#)))))

         (defn ~(symbol (str "insert-" (singular (str table-name))))
           ~(format "Insert the %s row into the database." (singular (str table-name)))
           [~'row & ~'opts] (run1 (apply insert ~(as-keyword table#) [~'row] ~'opts)))

         (defn ~(symbol (str "insert-" (str table-name)))
           ~(format "Insert the %s rows into the database." (singular (str table-name)))
           [~'rows & ~'opts] (run (apply insert ~(as-keyword table#) ~'rows ~'opts)))

         (defn ~(symbol (str "truncate-" table-name))
           ~(format "Truncate the %s database table." table-name)
           [& ~'opts] (:count (run1 (apply truncate ~(as-keyword table#) ~'opts))))

         (defn ~(symbol (str "update-" (singular (str table-name))))
           ~(format "Update the %s row in the database." (singular (str table-name)))
           [~'row & ~'opts] (run1 (apply update ~(as-keyword table#) ~'row ~'opts)))

         (defn ~(symbol (str "save-" (singular (str table-name))))
           ~(format "Save the %s row to the database." (singular (str table-name)))
           [~'row & ~'opts]
           (or (apply ~(symbol (str "update-" (singular (str table-name)))) ~'row ~'opts)
               (apply ~(symbol (str "insert-" (singular (str table-name)))) ~'row ~'opts)))

         (defquery ~table-name
           ~(format "Select %s from the database table." table-name)
           [& {:as ~'opts}]
           (-> (select *)
               (from ~(as-keyword table#))
               (paginate :page (:page ~'opts) :per-page (:per-page ~'opts))
               (when-> (:order-by ~'opts)
                       (order-by [(:order-by ~'opts)]))))

         ~@(for [column (vals (:column table#))
                 :let [column-name (name (:name column))]]
             (do
               `(do (defquery ~(symbol (str table-name "-by-" column-name))
                      ~(format "Find all %s by %s." table-name column-name)
                      [~'value & ~'opts]
                      (let [column# (first (meta/columns (jdbc/connection) :schema ~(:schema table#) :table ~(:name table#) :name ~(:name column)))]
                        (assert column#)
                        (-> (select *)
                            (from ~(as-keyword table#))
                            (where `(= ~(:name column#) ~(io/encode-column column# ~'value))))))
                    (defn ~(symbol (str (singular table-name) "-by-" column-name))
                      ~(format "Find the first %s by %s." (singular table-name) column-name)
                      [& ~'args]
                      (first (apply ~(symbol (str table-name "-by-" column-name)) ~'args)))))))))

(defmacro with-connection
  "Evaluates `body` with a connection to `db-name`."
  [db-name & body] `(connection/with-connection ~db-name ~@body))
