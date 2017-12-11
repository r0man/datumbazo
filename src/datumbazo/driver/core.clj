(ns datumbazo.driver.core
  (:require [clojure.string :as str]
            [sqlingvo.core :as sql :refer [ast sql]]
            [clojure.spec.alpha :as s]))

(defprotocol IConnection
  (-connect [driver opts])
  (-connection [driver])
  (-disconnect [driver]))

(defprotocol IFetch
  (-fetch [driver sql opts]))

(defprotocol IExecute
  (-execute [driver sql opts]))

(defprotocol IPrepareStatement
  (-prepare-statement [driver sql opts]))

(defprotocol ITransaction
  (-begin [driver opts])
  (-commit [driver opts])
  (-rollback [driver opts]))

(defmulti find-driver
  "Find the driver for `db`."
  (fn [db & [opts]] (:backend db)))

(defn row-count
  "Normalize into a record, with the count of affected rows."
  [result]
  [{:count
    (if (sequential? result)
      (first result)
      result)}])

(defn load-drivers
  "Load the driver namespaces."
  []
  (try
    (doseq [ns '[datumbazo.driver.jdbc.clojure
                 datumbazo.driver.jdbc.funcool]]
      (try (require ns)
           (catch Exception _)))))

(defn connection
  "Return the current connection to `db`."
  [db]
  (-connection db))

(s/fdef connection
  :args (s/cat :db sql/db?))

(defn connected?
  "Returns true if `db` is connected, otherwise false."
  [db]
  (some? (connection db)))

(s/fdef connected?
  :args (s/cat :db sql/db?))

(defn begin
  "Begin a new `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-begin db opts))

(s/fdef begin
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn commit
  "Commit the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-commit db opts))

(s/fdef commit
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn connect
  "Connect to `db` using `opts`."
  [db & [opts]]
  {:pre [(not (connected? db))]}
  (-connect db opts))

(s/fdef connect
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn disconnect
  "Disconnect from `db`."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-disconnect db))

(s/fdef disconnect
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn prepare-statement
  "Return a prepared statement for `sql`."
  [db sql & [opts]]
  {:pre [(connected? db)]}
  (-prepare-statement db sql opts))

(s/fdef prepare-statement
  :args (s/cat :db sql/db? :sql any? :opts (s/? (s/nilable map?))))

(defn rollback
  "Rollback the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-rollback db opts))

(s/fdef rollback
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn with-connection*
  "Open a database connection, call `f` with the connected `db` as
  argument and close the connection again."
  [db f & [opts]]
  (if (connected? db)
    (f db)
    (let [db (connect db opts)]
      (try (f db)
           (finally (disconnect db))))))

(s/fdef with-connection*
  :args (s/cat :db sql/db? :f ifn? :opts (s/? (s/nilable map?))))

(defmacro with-connection
  "Open a database connection, bind the connected `db` to `db-sym`,
  evaluate `body` and close the connection again."
  [[db-sym db & [opts]] & body]
  `(with-connection* ~db (fn [~db-sym] ~@body) ~opts))

(defn with-transaction*
  "Start a new `db` transaction call `f` with `db` as argument and
  commit the transaction. If `f` throws any exception the transaction
  gets rolled back."
  [db f & [opts]]
  {:pre [(connected? db)]}
  (let [db (begin db opts)]
    (try
      (let [ret (f db)]
        (commit db opts)
        ret)
      (catch Throwable t
        (rollback db)
        (throw t)))))

(s/fdef with-transaction*
  :args (s/cat :db sql/db? :f ifn? :opts (s/? (s/nilable map?))))

(defmacro with-transaction
  "Start a new `db` transaction, bind `db` to `db-sym` and evaluate
  `body` within the transaction."
  [[db-sym db & [opts]] & body]
  `(with-connection [db# ~db]
     (with-transaction* db# (fn [~db-sym] ~@body) ~opts)))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [ast (ast stmt)]
    (with-connection [db (:db ast)]
      (with-open [stmt (prepare-statement db (sql ast))]
        (if (.startsWith (str stmt) (str/replace (first (sql ast)) #"\?.*" ""))
          (str stmt)
          (throw (UnsupportedOperationException.
                  "Sorry, sql-str not supported by SQL driver.")))))))

(defn execute-sql-query
  "Execute a SQL query."
  [db sql & [opts]]
  (with-connection [db db]
    (try (-fetch (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL query."
                           {:sql sql :opts opts} e))))))

(s/fdef execute-sql-query
  :args (s/cat :db sql/db? :sql any? :opts (s/? (s/nilable map?))))

(defn execute-sql-statement
  "Execute a SQL statement."
  [db sql & [opts]]
  (with-connection [db db]
    (try (-execute (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL statement."
                           {:sql sql :opts opts} e))))))

(s/fdef execute-sql-statement
  :args (s/cat :db sql/db? :sql any? :opts (s/? (s/nilable map?))))

(defn- execute-fn
  "Return the SQL execution fn for `ast`."
  [ast]
  (case (:op ast)
    :create-table
    execute-sql-statement
    :delete
    (if (:returning ast)
      execute-sql-query
      execute-sql-statement)
    :except
    execute-sql-query
    :explain
    execute-sql-query
    :insert
    (if (:returning ast)
      execute-sql-query
      execute-sql-statement)
    :intersect
    execute-sql-query
    :select
    execute-sql-query
    :union
    execute-sql-query
    :update
    (if (:returning ast)
      execute-sql-query
      execute-sql-statement)
    :values
    execute-sql-query
    :with
    (execute-fn (:query ast))
    execute-sql-statement))

(defn execute
  "Execute `stmt` against a database."
  [stmt & [opts]]
  (let [{:keys [db] :as ast} (ast stmt)]
    (let [sql (sql ast), execute-fn (execute-fn ast)]
      (with-meta
        (case (:op ast)
          :create-table
          (if (seq (rest sql))
            ;; TODO: sql-str only works with PostgreSQL driver
            (execute-fn db (sql-str stmt) opts)
            (execute-fn db sql opts))
          (execute-fn db sql opts))
        {:datumbazo/stmt stmt
         :datumbazo/opts opts}))))
