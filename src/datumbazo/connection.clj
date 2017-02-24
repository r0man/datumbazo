(ns datumbazo.connection
  (:require [clojure.string :as str]
            [datumbazo.driver.core :as driver]
            [sqlingvo.core :refer [ast sql]])
  (:import java.sql.Connection
           java.sql.PreparedStatement
           sqlingvo.expr.Stmt
           sqlingvo.db.Database))

(defn connection
  "Return the current connection to `db`."
  [db]
  (driver/-connection (:driver db)))

(defn connected?
  "Returns true if `db` is connected, otherwise false."
  [db]
  (some? (connection db)))

(defn begin
  "Begin a new `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(driver/-begin % opts)))

(defn commit
  "Commit the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(driver/-commit % opts)))

(defn connect
  "Connect to `db` using `opts`."
  [db & [opts]]
  {:pre [(not (connected? db))]}
  (update db :driver #(driver/-connect % opts)))

(defn disconnect
  "Disconnect from `db`."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver driver/-disconnect))

(defn prepare-statement
  "Return a prepared statement for `sql`."
  [db sql & [opts]]
  {:pre [(connected? db)]}
  (driver/-prepare-statement (:driver db) sql opts))

(defn rollback
  "Rollback the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(driver/-rollback % opts)))

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
  `(with-transaction* ~db (fn [~db-sym] ~@body) ~opts))

(defn execute-sql-query
  "Execute a SQL query."
  [db sql & [opts]]
  (with-connection [db db]
    (try (driver/-fetch (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL query."
                           {:sql sql :opts opts} e))))))

(defn execute-sql-statement
  "Execute a SQL statement."
  [db sql & [opts]]
  (with-connection [db db]
    (try (driver/-execute (:driver db) sql opts)
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
    (let [sql (sql ast)
          execute (execute-fn ast)]
      (case (:op ast)
        :create-table
        (if (seq (rest sql))
          ;; TODO: sql-str only works with PostgreSQL driver
          (execute db (sql-str stmt) opts)
          (execute db sql opts))
        (execute db sql opts)))))
