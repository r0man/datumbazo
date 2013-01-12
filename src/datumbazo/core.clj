(ns datumbazo.core
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.algo.monads :refer [state-m m-seq with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.connection :as connection]
            [datumbazo.io :as io]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [immigrate]]
            [inflections.core :refer [hyphenize singular]]
            [inflections.util :refer [parse-integer]]
            [pallet.thread-expr :refer [when->]]
            [sqlingvo.util :refer [as-identifier as-keyword parse-table parse-expr concat-in]]
            datumbazo.json
            sqlingvo.core))

(immigrate 'sqlingvo.core)

(def ^:dynamic *per-page* 25)

(defn column
  "Add a database column to `table`."
  [name type & {:as options}]
  (let [column (assoc options :op :column :name name :type type)]
    (fn [table]
      [nil (-> (update-in table [:columns] #(concat %1 [(:name column)]))
               (assoc-in [:column (:name column)]
                         (assoc column
                           :schema (:schema table)
                           :table (:name table))))])))

(defn columns
  "Returns the columns of `table`."
  [table] (map (:column table) (:columns table)))

(defn description
  "Add `text` as description to to `table`."
  [text]
  (fn [table]
    [text (assoc table :doc text)]))

(defn run
  "Compile and run `stmt` against the current clojure.java.jdbc
  database connection."
  [stmt]
  (let [ast (ast stmt)]
    (map (apply comp (reverse (concat [io/decode-row]
                                      (:transform (:table ast))
                                      (mapcat :transform (:from ast)))))
         (sqlingvo.core/run stmt))))

(defn run1
  "Run `stmt` against the current clojure.java.jdbc database
  connection and return the first row."
  [stmt] (first (run stmt)))

(defn table
  "Make a new table."
  [name & body]
  (fn [table]
    [nil (merge table (second ((chain-state body) (parse-table name))))]))

(defn transform
  "Add the transformation fn `f` to `table`."
  [f]
  (fn [table]
    [nil (update-in table [:transform] concat [f])]))

(defn update
  "Returns a fn that builds a UPDATE statement."
  [table row & body]
  (let [table (parse-table table)]
    (fn [stmt]
      (let [[_ update]
            ((chain-state body)
             {:op :update
              :table table
              :exprs (if (sequential? row) (map parse-expr row))
              :row (if (map? row) (io/encode-row table row))})]
        [update (assoc stmt (:op update) update)]))))

(defn values
  "Returns a fn that adds a VALUES clause to an SQL statement."
  [values]
  (fn [stmt]
    [nil (case values
           :default (assoc stmt :default-values true)
           (concat-in
            stmt [:values]
            (io/encode-rows
             (:table stmt)
             (if (sequential? values) values [values]))))]))

(defn count-all
  "Count all rows in the database `table`."
  [table] (:count (run1 (select ['(count *)] (from table)))))

(defn paginate
  "Add LIMIT and OFFSET clauses to `query` according to
  `page` (default: 1) and `per-page` (default: `*per-page*`)."
  [page & [per-page]]
  (let [page (parse-integer (or page 1))
        per-page (parse-integer (or per-page *per-page*))]
    (fn [stmt]
      ((chain-state
        [(limit per-page)
         (offset (* (dec page) (or per-page *per-page*)))])
       stmt) )))

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
  (let [table# (eval `(second ((table ~(keyword table-name) ~@body) {})))
        singular# (singular (str table-name))
        symbol# (symbol (str table-name "-table"))]
    `(do (def ~symbol#
           (second ((table ~(keyword table-name) ~@body) {})))

         (defn ~(symbol (str "drop-" table-name))
           ~(format "Drop the %s database table." table-name)
           [& ~'body] (:count (run1 (apply drop-table [~symbol#] ~'body))))

         (defn ~(symbol (str "delete-" table-name))
           ~(format "Delete all rows in the %s database table." table-name)
           [& ~'body] (:count (run1 (apply delete ~symbol# ~'body))))

         (defn ~(symbol (str "insert-" singular#))
           ~(format "Insert the %s row into the database." singular#)
           [~'row & ~'opts]
           (run1 (sqlingvo.core/insert ~symbol# []
                   (values (io/encode-row ~symbol# ~'row))
                   (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#))))))

         (defn ~(symbol (str "insert-" (str table-name)))
           ~(format "Insert the %s rows into the database." singular#)
           [~'rows & ~'opts]
           (run (sqlingvo.core/insert ~symbol# []
                  (values (io/encode-rows ~symbol# ~'rows))
                  (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#))))))

         (defn ~(symbol (str "truncate-" table-name))
           ~(format "Truncate the %s database table." table-name)
           [& ~'body] (:count (run1 (apply truncate [~symbol#] ~'body))))

         (defn ~(symbol (str "update-" singular#))
           ~(format "Update the %s row in the database." singular#)
           [~'row & ~'opts]
           (let [pks# (meta/primary-keys (jdbc/connection) :schema (:schema ~symbol#) :table (:name ~symbol#))
                 pk-keys# (map :name pks#)
                 pk-vals# (map ~'row pk-keys#)]
             (run1 (sqlingvo.core/update ~symbol# (io/encode-row ~symbol# ~'row)
                     (where (cons 'and (map #(list '= %1 %2) pk-keys# pk-vals#)))
                     (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#)))))))

         (defn ~(symbol (str "save-" singular#))
           ~(format "Save the %s row to the database." singular#)
           [~'row & ~'opts]
           (or (apply ~(symbol (str "update-" singular#)) ~'row ~'opts)
               (apply ~(symbol (str "insert-" singular#)) ~'row ~'opts)))

         (defquery ~table-name
           ~(format "Select %s from the database table." table-name)
           [& {:as ~'opts}]
           (select (remove #(= true (:hidden? %1)) (columns ~symbol#))
             (from ~symbol#)
             (paginate (:page ~'opts) (:per-page ~'opts))
             (order-by (:order-by ~'opts))))

         ~@(for [column (vals (:column table#)) :let [column-name (name (:name column))]]
             (do
               `(do (defquery ~(symbol (str table-name "-by-" column-name))
                      ~(format "Find all %s by %s." table-name column-name)
                      [~'value & {:as ~'opts}]
                      (let [column# (first (meta/columns (jdbc/connection) :schema ~(:schema table#) :table ~(:name table#) :name ~(:name column)))]
                        (select (remove #(= true (:hidden? %1)) (columns ~symbol#))
                          (from ~symbol#)
                          (where `(= ~(:name column#) ~(io/encode-column column# ~'value)))
                          (paginate (:page ~'opts) (:per-page ~'opts)))))
                    (defn ~(symbol (str (singular table-name) "-by-" column-name))
                      ~(format "Find the first %s by %s." (singular table-name) column-name)
                      [& ~'args]
                      (first (apply ~(symbol (str table-name "-by-" column-name)) ~'args)))))))))

(defmacro with-connection
  "Evaluates `body` with a connection to `db-name`."
  [db-name & body] `(connection/with-connection ~db-name ~@body))
