(ns datumbazo.core
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.algo.monads :refer [state-m m-seq with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.connection :refer [with-connection]]
            [datumbazo.io :as io]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [immigrate]]
            [inflections.core :refer [hyphenize singular]]
            [inflections.util :refer [parse-integer]]
            [sqlingvo.util :refer [as-identifier as-keyword parse-table parse-expr concat-in]]
            sqlingvo.core))

(immigrate 'sqlingvo.core)

(def ^:dynamic *page* nil)
(def ^:dynamic *per-page* 25)

(defn- prepare-stmt
  "Compile `stmt` and return a java.sql.PreparedStatement from `db`."
  [db stmt]
  (with-connection [db db]
    (let [[sql & args] (sql stmt)
          stmt (jdbc/prepare-statement (:connection db) sql)]
      (doall (map-indexed (fn [i v] (.setObject stmt (inc i) v)) args))
      stmt)))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [db stmt & opts]
  (let [sql (first (apply sql stmt opts))
        stmt (prepare-stmt db stmt)]
    (if (.startsWith (str stmt) (str/replace sql #"\?.*" ""))
      (str stmt)
      (throw (UnsupportedOperationException. "Sorry, sql-str not supported by SQL driver.")))))

(defn- run-query
  [db compiled  & {:keys [identifiers transaction?]}]
  (let [query #(jdbc/query %1 compiled :identifiers (or identifiers hyphenize))]
    (if transaction?
      (jdbc/db-transaction [t-db db] (query t-db))
      (query db))))

(defn- run-prepared
  [db compiled & {:keys [identifiers transaction?]}]
  (->> (jdbc/db-do-prepared db transaction? (first compiled) (rest compiled))
       (map #(hash-map :count %1))))

(defn run*
  "Compile and run `stmt` against the database and return the rows."
  [db stmt & {:keys [entities identifiers transaction?]}]
  (with-connection [db db]
    (let [{:keys [op returning] :as ast} (ast stmt)
          compiled (sql ast :entities entities)]
      (cond
       (= :select op)
       (run-query db compiled :identifiers identifiers :transaction? transaction?)
       returning
       (run-query db compiled :identifiers identifiers :transaction? transaction?)
       :else (run-prepared db compiled :identifiers identifiers :transaction? transaction?)))))

;; ----------------------------------------------------------------------------------------------------------

(defn columns
  "Returns the columns of `table`."
  [table] (map (:column table) (:columns table)))

(defn description
  "Add `text` as description to to `table`."
  [text]
  (fn [table]
    [text (assoc table :doc text)]))

(defn- apply-preparation [ast]
  (let [prepare (concat (:prepare ast) (:prepare (:table ast)))
        prepare (if (empty? prepare) identity (apply comp prepare))]
    (case (:op ast)
      :insert (update-in ast [:values] #(map prepare %1))
      :update (update-in ast [:row] prepare)
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
  [db stmt & opts]
  (let [ast (ast stmt)]
    (->> (apply run* db (apply-preparation ast) opts)
         (apply-transformation ast))))

(defn run1
  "Run `stmt` against the current clojure.java.jdbc database
  connection and return the first row."
  [db stmt & opts]
  (first (apply run db stmt opts)))

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
  [db table] (:count (run1 db (select ['(count *)] (from table)))))

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
             (->> (run db# query#)
                  (map (or ~map-fn identity))))))))

(defmacro defquery1 [name doc args body & [map-fn]]
  (let [query-sym (symbol (str name "*"))]
    `(do (defn ~query-sym ~doc ~args
           ~body)
         (defn ~name ~doc [& ~'args]
           (let [db# (first ~'args)
                 query# (apply ~query-sym ~'args)]
             ((or ~map-fn identity)
              (run1 db# query#)))))))

(defmacro with-rollback
  "Evaluate `body` within a transaction on `db` and rollback
  afterwards."
  [[symbol db] & body]
  `(with-connection [db# ~db]
     (jdbc/db-transaction
      [~symbol db#]
      (jdbc/db-set-rollback-only! ~symbol)
      ~@body)))

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
           [~'db & ~'body] (:count (run1 ~'db (apply drop-table [~symbol#] ~'body))))

         (defn ~(symbol (str "delete-" table-name))
           ~(format "Delete all rows in the %s database table." table-name)
           [~'db & ~'body] (:count (run1 ~'db (apply delete ~symbol# ~'body))))

         (defn ~(symbol (str "delete-" singular#))
           ~(format "Delete the %s from the database table." singular#)
           [~'db ~'row] (:count (run1 ~'db (delete ~symbol#
                                             (from ~(keyword table-name))
                                             (where `(= :id ~(:id ~'row)))))))

         (defn ~(symbol (str "insert-" singular#))
           ~(format "Insert the %s row into the database." singular#)
           [~'db ~'row & ~'opts]
           (run1 ~'db (sqlingvo.core/insert ~symbol# []
                        (values ~'row)
                        (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#)))
                        (prepare (partial io/encode-row ~'db ~symbol#))
                        (prepare (:prepare ~symbol#)))))

         (defn ~(symbol (str "insert-" (str table-name)))
           ~(format "Insert the %s rows into the database." singular#)
           [~'db ~'rows & ~'opts]
           (run ~'db (sqlingvo.core/insert ~symbol# []
                       (values ~'rows)
                       (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#)))
                       (prepare (partial io/encode-row ~'db ~symbol#))
                       (prepare (:prepare ~symbol#)))))

         (defn ~(symbol (str "truncate-" table-name))
           ~(format "Truncate the %s database table." table-name)
           [~'db & ~'body] (:count (run1 ~'db (apply truncate [~symbol#] ~'body))))

         (defn ~(symbol (str "update-" singular#))
           ~(format "Update the %s row in the database." singular#)
           [~'db ~'row & ~'opts]
           (let [pks# (meta/primary-keys ~'db :schema (or (:schema ~symbol#) :public) :table (:name ~symbol#))
                 pk-keys# (map :name pks#)
                 pk-vals# (map ~'row pk-keys#)]
             (run1 ~'db (update ~symbol# ~'row
                          (where (cons 'and (map #(list '= %1 %2) pk-keys# pk-vals#)))
                          (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#)))
                          (prepare (partial io/encode-row ~'db ~symbol#))
                          (prepare (:prepare ~symbol#))))))

         (defn ~(symbol (str "save-" singular#))
           ~(format "Save the %s row to the database." singular#)
           [~'db ~'row & ~'opts]
           (or (apply ~(symbol (str "update-" singular#)) ~'db ~'row ~'opts)
               (apply ~(symbol (str "insert-" singular#)) ~'db ~'row ~'opts)))

         (defquery ~table-name
           ~(format "Select %s from the database table." table-name)
           [~'db & [~'opts]]
           (select (remove #(= true (:hidden? %1)) (columns ~symbol#))
             (from ~symbol#)
             (paginate (:page ~'opts) (:per-page ~'opts))
             (order-by (:order-by ~'opts))))

         ~@(for [column (vals (:column table#)) :let [column-name (name (:name column))]]
             (do
               `(do (defquery ~(symbol (str table-name "-by-" column-name))
                      ~(format "Find all %s by %s." table-name column-name)
                      [~'db ~'value & [~'opts]]
                      (let [column# (first (meta/columns ~'db :schema (or ~(:schema table#) :public) :table ~(:name table#) :name ~(:name column) :entities (:entities ~'opts)))]
                        (fn [stmt#]
                          ((chain-state [(where `(= ~(keyword (str (name (:table column#)) "." (name (:name column#))))
                                                    ~(io/encode-column column# ~'value)))])
                           (ast (~(symbol (str table-name "*")) ~'db ~'opts))))))
                    (defn ~(symbol (str (singular table-name) "-by-" column-name))
                      ~(format "Find the first %s by %s." (singular table-name) column-name)
                      [~'db & ~'args]
                      (first (apply ~(symbol (str table-name "-by-" column-name)) ~'db ~'args)))))))))
