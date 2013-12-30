(ns datumbazo.core
  (:import java.sql.SQLException
           org.postgresql.PGConnection)
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.io :refer [reader]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [datumbazo.connection :refer [with-connection]]
            [datumbazo.io :as io]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [compact-map immigrate]]
            [inflections.core :refer [hyphenize singular]]
            [no.en.core :refer [parse-integer]]
            [sqlingvo.util :refer [parse-table parse-expr concat-in]]
            sqlingvo.core))

(immigrate 'sqlingvo.core)

(def ^:dynamic *page* nil)
(def ^:dynamic *per-page* 25)

(defn- prepare-stmt
  "Compile `stmt` and return a java.sql.PreparedStatement from `db`."
  [db stmt]
  (with-connection [db db]
    (let [[sql & args] (sql db stmt)
          stmt (jdbc/prepare-statement (:connection db) sql)]
      (doall (map-indexed (fn [i v] (.setObject stmt (inc i) v)) args))
      stmt)))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [db stmt]
  (let [sql (first (sql db stmt))
        stmt (prepare-stmt db stmt)]
    (if (.startsWith (str stmt) (str/replace sql #"\?.*" ""))
      (str stmt)
      (throw (UnsupportedOperationException. "Sorry, sql-str not supported by SQL driver.")))))

(defn- run-copy
  [db ast  & {:keys [transaction?]}]
  ;; TODO: Get rid of sql-str
  (let [compiled (sql-str db ast)
        stmt (.prepareStatement (:connection db) compiled)]
    (.execute stmt)))

(defn- run-query
  [db ast  & {:keys [transaction?]}]
  (let [compiled (sql db ast)
        identifiers #(sql-keyword db %1)
        query #(jdbc/query %1 compiled :identifiers identifiers)]
    (if transaction?
      (jdbc/with-db-transaction [t-db db] (query t-db))
      (query db))))

(defn- run-prepared
  [db ast & {:keys [transaction?]}]
  (let [compiled (sql db ast)]
    (->> (jdbc/db-do-prepared db transaction? (first compiled) (rest compiled))
         (map #(hash-map :count %1)))))

(defn run*
  "Compile and run `stmt` against the database and return the rows."
  [db stmt & {:keys [transaction?]}]
  (with-connection [db db]
    (let [{:keys [op returning] :as ast} (ast stmt)]
      (try (cond
            (= :copy op)
            (run-copy db ast :transaction? transaction?)
            (= :select op)
            (run-query db ast :transaction? transaction?)
            (and (= :with op)
                 (or (= :select (:op (:query ast)))
                     (:returning (:query ast))))
            (run-query db ast :transaction? transaction?)
            returning
            (run-query db ast :transaction? transaction?)
            :else (run-prepared db ast :transaction? transaction?))
           (catch Exception e
             (if (or (instance? SQLException e)
                     (instance? SQLException (.getCause e)))
               (throw (ex-info (format "Can't execute SQL statement: %s\n%s"
                                       (pr-str (sql stmt))
                                       (.getMessage e))
                               ast e))
               (throw e)))))))

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
         ((if (= :delete (:op ast))
            identity
            #(apply-transformation ast %1))))))

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
     (jdbc/with-db-transaction
      [~symbol db#]
      (jdbc/db-set-rollback-only! ~symbol)
      ~@body)))

(defn update-columns-best-row-identifiers [db table row]
  (let [columns (meta/best-row-identifiers db :schema (or (:schema table) :public) :table (:name table) :nullable false)
        row (compact-map (select-keys row (map :name columns)))]
    (if-not (empty? row) row)))

(defn update-columns-unique-columns [db table row]
  (let [columns (meta/unique-columns db :schema (or (:schema table) :public) :table (:name table) :nullable false)
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
    (select [:*]
      (from (:name table))
      (where (primary-key-clause db table row)))))

(defmacro deftable
  "Define a database table."
  [table-name doc & body]
  (let [table# (eval `(second ((table ~(keyword table-name) ~@body) {})))
        singular# (singular (str table-name))
        symbol# (symbol (str table-name "-table"))]
    `(do (def ~symbol#
           (second ((table ~(keyword table-name) ~@body) {})))

         (defquery ~table-name
           ~(format "Select %s from the database table." table-name)
           [~'db & [~'opts]]
           (select (remove #(= true (:hidden? %1)) (columns ~symbol#))
             (from ~symbol#)
             (paginate (:page ~'opts) (:per-page ~'opts))
             (order-by (:order-by ~'opts))))

         (defquery1 ~(symbol (str (singular table-name)  "-by-pk"))
           ~(format "Find the %s by primary key." (singular table-name))
           [~'db ~'row & [~'opts]]
           (compose (~(symbol (str table-name "*")) ~'db)
                    (where (primary-key-clause ~'db ~symbol# ~'row))))

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
           (run1 ~'db (update ~symbol# ~'row
                        (where (primary-key-clause ~'db ~symbol# ~'row))
                        (apply returning (remove #(= true (:hidden? %1)) (columns ~symbol#)))
                        (prepare (partial io/encode-row ~'db ~symbol#))
                        (prepare (:prepare ~symbol#)))))

         (defn ~(symbol (str "save-" singular#))
           ~(format "Save the %s row to the database." singular#)
           [~'db ~'row & ~'opts]
           (jdbc/with-db-transaction
            [~'db ~'db]
            (or (apply ~(symbol (str "update-" singular#)) ~'db ~'row ~'opts)
                (apply ~(symbol (str "insert-" singular#)) ~'db ~'row ~'opts))))

         ~@(for [column (vals (:column table#)) :let [column-name (name (:name column))]]
             (do
               `(do (defquery ~(symbol (str table-name "-by-" column-name))
                      ~(format "Find all %s by %s." table-name column-name)
                      [~'db ~'value & [~'opts]]
                      (let [column# (first (meta/columns ~'db :schema (or ~(:schema table#) :public) :table ~(:name table#) :name ~(:name column)))]
                        (fn [stmt#]
                          ((chain-state [(where `(= ~(keyword (str (name (:table column#)) "." (name (:name column#))))
                                                    ~(io/encode-column column# ~'value)))])
                           (ast (~(symbol (str table-name "*")) ~'db ~'opts))))))
                    (defn ~(symbol (str (singular table-name) "-by-" column-name))
                      ~(format "Find the first %s by %s." (singular table-name) column-name)
                      [~'db & ~'args]
                      (first (apply ~(symbol (str table-name "-by-" column-name)) ~'db ~'args)))))))))
