(ns datumbazo.driver.core
  (:require [clojure.string :as str]
            [sqlingvo.core :refer [ast sql]]))

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
  (-connection (:driver db)))

(defn connected?
  "Returns true if `db` is connected, otherwise false."
  [db]
  (some? (connection db)))

(defn begin
  "Begin a new `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-begin db opts))

(defn commit
  "Commit the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-commit db opts))

(defn connect
  "Connect to `db` using `opts`."
  [db & [opts]]
  {:pre [(not (connected? db))]}
  (-connect db opts))

(defn disconnect
  "Disconnect from `db`."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-disconnect db))

(defn prepare-statement
  "Return a prepared statement for `sql`."
  [db sql & [opts]]
  {:pre [(connected? db)]}
  (-prepare-statement db sql opts))

(defn rollback
  "Rollback the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (-rollback db opts))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [ast (ast stmt)]
    (with-open [stmt (prepare-statement (:db ast) (sql ast))]
      (if (.startsWith (str stmt) (str/replace (first (sql ast)) #"\?.*" ""))
        (str stmt)
        (throw (UnsupportedOperationException.
                "Sorry, sql-str not supported by SQL driver."))))))

(defn with-connection*
  "Open a database connection, call `f` with the connected `db` as
  argument and close the connection again."
  [db f & [opts]]
  (if (connected? db)
    (f db)
    (let [db (connect db opts)]
      (try (f db)
           (finally (disconnect db))))))

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

(defmacro with-transaction
  "Start a new `db` transaction, bind `db` to `db-sym` and evaluate
  `body` within the transaction."
  [[db-sym db & [opts]] & body]
  `(with-connection [db# ~db]
     (with-transaction* db# (fn [~db-sym] ~@body) ~opts)))

(defn execute-sql-query
  "Execute a SQL query."
  [db sql & [opts]]
  (with-connection [db db]
    (try (-fetch (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL query."
                           {:sql sql :opts opts} e))))))

(defn execute-sql-statement
  "Execute a SQL statement."
  [db sql & [opts]]
  (with-connection [db db]
    (try (-execute (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL statement."
                           {:sql sql :opts opts} e))))))

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
