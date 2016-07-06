(ns datumbazo.core
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.associations]
            [datumbazo.db]
            [datumbazo.driver.core :as driver]
            [datumbazo.io :as io]
            [datumbazo.meta :as meta]
            [datumbazo.pool.core :as pool]
            [datumbazo.util :refer [compact-map immigrate]]
            [inflections.core :refer [singular]]
            [no.en.core :refer [parse-integer]]
            [potemkin :refer [import-vars]]
            [sqlingvo.core]
            [sqlingvo.expr :refer [parse-table]]))

(import-vars
 [datumbazo.associations
  belongs-to
  has-many]
 [datumbazo.connection
  begin
  commit
  connect
  connection
  disconnect
  execute
  prepare-statement
  rollback
  sql-str
  with-connection
  with-transaction]
 [datumbazo.db
  new-db
  with-db])

(immigrate 'sqlingvo.core)

(def ^:dynamic *page* nil)
(def ^:dynamic *per-page* 25)

(declare run)

(defn columns
  "Returns the columns of `table`."
  [table] (map (:column table) (:columns table)))

(defn prefix
  "Prefix `kw` with `namespace`."
  [namespace kw]
  (keyword (str (name namespace) "."
                (name kw))))

(defn column-keys
  "Returns the column keys of `table`."
  [table & {:keys [prefix?]}]
  (map #(if prefix?
          (prefix (:name table) (:name %))
          (:name %))
       (columns table)))

(defn select-columns
  "Select the columns of `table` from `row`."
  [table row] (select-keys row (column-keys table)))

(defn description
  "Add `text` as description to to `table`."
  [text]
  (fn [table]
    [text (assoc table :doc text)]))

(defn- raw-row [row]
  (->> (for [[k v] row
             :when (= :constant (:op v))]
         [k (:val v)])
       (into {})))

(defn- lift-row [table row f]
  (reduce
   (fn [row [k v]]
     (-> row
         (assoc-in [k :val] v)
         (assoc-in [k :form] v)))
   row (f (raw-row row))))

(defn- apply-preparation [ast]
  (let [prepare (concat (:prepare ast) (:prepare (:table ast)))
        prepare (if (empty? prepare) identity (apply comp prepare))]
    (case (:op ast)
      :insert (update-in ast [:values :values] #(map (fn [row] (lift-row (:table ast) row prepare)) %1))
      :update (update-in ast [:row] #(lift-row (:table ast) % prepare))
      ast)))

(defn- apply-transformation [ast rows]
  (let [transformations
        (concat [io/decode-row]
                (:transform ast)
                (:transform (:table ast))
                (mapcat :transform (:from ast)))]
    (map (apply comp (reverse transformations)) rows)))

(defn prepare
  "Add the preparation fn `f` to `table`."
  [f]
  (fn [table]
    (let [preparation (remove nil? (if (sequential? f) f [f]))]
      [nil (update-in table [:prepare] concat preparation)])))

(defn run
  "Compile and run `stmt` against the current clojure.java.jdbc
  database connection."
  [stmt & [opts]]
  (let [ast (ast stmt)]
    (->> (execute (apply-preparation ast) opts)
         ((if (= :delete (:op ast))
            identity
            #(apply-transformation ast %1))))))

(defn run1
  "Run `stmt` against the current clojure.java.jdbc database
  connection and return the first row."
  [stmt & opts]
  (first (apply run stmt opts)))

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

(defn count-all
  "Count all rows in the database `table`."
  [db table] (:count (run1 (select db ['(count *)] (from table)))))

(defn paginate
  "Add LIMIT and OFFSET clauses to `query` calculated from `page` and
  `per-page.`"
  [page & [limit per-page]]
  (let [page (parse-integer (or page *page*))
        per-page (parse-integer (or limit per-page *per-page*))]
    (fn [stmt]
      (if page
        ((chain-state
          [(sqlingvo.core/limit per-page)
           (offset (* (dec page) (or per-page *per-page*)))])
         stmt)
        [nil stmt]))))

(defmacro defquery [name doc args body & [map-fn]]
  (let [query-sym (symbol (str name "*"))]
    `(do (defn ~query-sym ~doc ~args
           ~body)
         (defn ~name ~doc [& ~'args]
           (let [db# (first ~'args)
                 query# (apply ~query-sym ~'args)]
             (->> (run query#)
                  (map (or ~map-fn identity))))))))

(defmacro defquery1 [name doc args body & [map-fn]]
  (let [query-sym (symbol (str name "*"))]
    `(do (defn ~query-sym ~doc ~args
           ~body)
         (defn ~name ~doc [& ~'args]
           (let [db# (first ~'args)
                 query# (apply ~query-sym ~'args)]
             ((or ~map-fn identity)
              (run1 query#)))))))

(defn update-columns-best-row-identifiers [db table row]
  (let [columns (meta/best-row-identifiers db {:schema (or (:schema table) :public) :table (:name table) :nullable false})
        row (compact-map (select-keys row (map :name columns)))]
    (if-not (empty? row) row)))

(defn update-columns-unique-columns [db table row]
  (let [columns (meta/unique-columns db {:schema (or (:schema table) :public) :table (:name table) :nullable false})
        row (compact-map (select-keys row (map :name columns)))]
    (if-not (empty? row) row)))

(defn where-clause-columns [db table row]
  (or (update-columns-best-row-identifiers db table row)
      (update-columns-unique-columns db table row)))

(defn update-clause [db table row]
  (let [row (where-clause-columns db table row)]
    (where (cons 'and (map #(list '= %1 %2) (keys row) (vals row))))))

(defn primary-key-clause [db table row]
  (let [row (io/encode-row db table row)
        primary-key (or (:primary-key table)
                        (map :name (filter :primary-key? (vals (:column table)))))]
    (cons 'and (map #(list := (keyword (str (name (:name table)) "." (name %1)))
                           (get row %1))
                    primary-key))))

(defn find-by-primary-key [db table row]
  (let [primary-key (or (:primary-key table)
                        (map :name (filter :primary-key? (vals (:column table)))))]
    (select db [:*]
      (from (:name table))
      (where (primary-key-clause db table row)))))

(defn select-rows
  "Select rows from the database `table`."
  [db table & [opts]]
  (select db (remove #(= true (:hidden? %1)) (columns table))
    (from table)
    (paginate (:page opts) (:per-page opts))
    (order-by (:order-by opts))))

(defn insert-row
  "Insert `row` into the database `table`."
  [db table row & [opts]]
  (run1 (sqlingvo.core/insert
            db table []
          (values [(select-columns table row)])
          (apply returning (remove #(= true (:hidden? %1)) (columns table)))
          (prepare (partial io/encode-row db table))
          (prepare (:prepare table)))))

(defn insert-rows
  "Insert `rows` into the database `table`."
  [db table rows & [opts]]
  (run (sqlingvo.core/insert
           db table []
         (values (map #(select-columns table %) rows))
         (apply returning (remove #(= true (:hidden? %1)) (columns table)))
         (prepare (partial io/encode-row db table))
         (prepare (:prepare table)))))

(defn update-row
  "Update the `row` in the database `table`."
  [db table row & [opts]]
  (run1 (update db table (select-columns table row)
                (where (primary-key-clause db table row))
                (apply returning (remove #(= true (:hidden? %1)) (columns table)))
                (prepare (partial io/encode-row db table))
                (prepare (:prepare table)))))

(defmacro deftable
  "Define a database table."
  [table-sym doc & body]
  (let [table# (eval `(second ((table ~(keyword table-sym) ~@body) {})))
        singular# (singular (str table-sym))
        symbol# (symbol (str table-sym "-table"))]
    `(do (def ~symbol#
           (second ((table ~(keyword table-sym) ~@body) {})))

         (defquery ~table-sym
           ~(format "Select %s from the database table." table-sym)
           [~'db & [~'opts]]
           (select-rows ~'db ~symbol# ~'opts))

         (defquery1 ~(symbol (str (singular table-sym)  "-by-pk"))
           ~(format "Find the %s by primary key." (singular table-sym))
           [~'db ~'row & [~'opts]]
           (compose (~(symbol (str table-sym "*")) ~'db)
                    (where (primary-key-clause ~'db ~symbol# ~'row))))

         (defn ~(symbol (str "drop-" table-sym))
           ~(format "Drop the %s database table." table-sym)
           [~'db & ~'body]
           (:count (run1 (apply drop-table ~'db [~symbol#] ~'body))))

         (defn ~(symbol (str "delete-" table-sym))
           ~(format "Delete all rows in the %s database table." table-sym)
           [~'db & ~'body] (:count (run1 (apply delete ~'db ~symbol# ~'body))))

         (defn ~(symbol (str "delete-" singular#))
           ~(format "Delete the %s from the database table." singular#)
           [~'db ~'row] (:count (run1 (delete ~'db  ~symbol#
                                        (from ~(keyword table-sym))
                                        (where `(= :id ~(:id ~'row)))))))

         (defn ~(symbol (str "insert-" singular#))
           ~(format "Insert the %s row into the database." singular#)
           [~'db ~'row & ~'opts]
           (insert-row ~'db ~symbol# ~'row ~'opts))

         (defn ~(symbol (str "insert-" (str table-sym)))
           ~(format "Insert the %s rows into the database." singular#)
           [~'db ~'rows & ~'opts]
           (insert-rows ~'db ~symbol# ~'rows ~'opts))

         (defn ~(symbol (str "truncate-" table-sym))
           ~(format "Truncate the %s database table." table-sym)
           [~'db & ~'body] (:count (run1 (apply truncate ~'db [~symbol#] ~'body))))

         (defn ~(symbol (str "update-" singular#))
           ~(format "Update the %s row in the database." singular#)
           [~'db ~'row & ~'opts]
           (update-row ~'db ~symbol# ~'row ~'opts))

         (defn ~(symbol (str "save-" singular#))
           ~(format "Save the %s row to the database." singular#)
           [~'db ~'row & ~'opts]
           (with-transaction
             [~'db ~'db]
             (or (apply ~(symbol (str "update-" singular#)) ~'db ~'row ~'opts)
                 (apply ~(symbol (str "insert-" singular#)) ~'db ~'row ~'opts))))

         ~@(for [column (vals (:column table#))
                 :let [column-name (name (:name column))]]
             (do
               `(do (defquery ~(symbol (str table-sym "-by-" column-name))
                      ~(format "Find all %s by %s." table-sym column-name)
                      [~'db ~'value & [~'opts]]
                      (let [column# (first (meta/columns ~'db {:schema (or ~(:schema table#) :public) :table ~(:name table#) :name ~(:name column)}))]
                        (fn [stmt#]
                          ((chain-state [(where `(= ~(keyword (str (name (:table column#)) "." (name (:name column#))))
                                                    ~(io/encode-column column# ~'value)))])
                           (ast (~(symbol (str table-sym "*")) ~'db ~'opts))))))
                    (defn ~(symbol (str (singular table-sym) "-by-" column-name))
                      ~(format "Find the first %s by %s." (singular table-sym) column-name)
                      [~'db & ~'args]
                      (first (apply ~(symbol (str table-sym "-by-" column-name)) ~'db ~'args)))))))))


(driver/load-drivers)
(pool/load-connection-pools)
