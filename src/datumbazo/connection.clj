(ns datumbazo.connection
  (:require [clojure.string :as str]
            [datumbazo.driver.core :as driver]
            [schema.core :as s]
            [sqlingvo.core :as sql])
  (:import java.sql.Connection
           java.sql.PreparedStatement
           sqlingvo.expr.Stmt
           sqlingvo.db.Database))

(s/defn connection :- (s/maybe Connection)
  "Return the current connection to `db`."
  [db :- Database]
  (driver/-connection (:driver db)))

(s/defn connected? :- s/Bool
  "Returns true if `db` is connected, otherwise false."
  [db :- Database]
  (some? (connection db)))

(s/defn begin :- Database
  "Begin a new `db` transaction."
  [db :- Database & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(driver/-begin % opts)))

(s/defn commit :- Database
  "Commit the current `db` transaction."
  [db :- Database & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(driver/-commit % opts)))

(s/defn connect :- Database
  "Connect to `db` using `opts`."
  [db :- Database & [opts]]
  {:pre [(not (connected? db))]}
  (update db :driver #(driver/-connect % opts)))

(s/defn disconnect :- Database
  "Disconnect from `db`."
  [db :- Database & [opts]]
  {:pre [(connected? db)]}
  (update db :driver driver/-disconnect))

(s/defn prepare-statement :- PreparedStatement
  "Return a prepared statement for `sql`."
  [db :- Database sql & [opts]]
  {:pre [(connected? db)]}
  (driver/-prepare-statement (:driver db) sql opts))

(s/defn rollback :- Database
  "Rollback the current `db` transaction."
  [db :- Database & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(driver/-rollback % opts)))

(s/defn sql-str :- s/Str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [ast (sql/ast stmt)
        sql (sql/sql ast)]
    (with-open [stmt (prepare-statement (:db ast) sql)]
      (if (.startsWith (str stmt) (str/replace (first sql) #"\?.*" ""))
        (str stmt)
        (throw (UnsupportedOperationException.
                "Sorry, sql-str not supported by SQL driver."))))))

(s/defn with-connection*
  "Open a database connection, call `f` with the connected `db` as
  argument and close the connection again."
  [db :- Database f & [opts]]
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

(s/defn with-transaction*
  "Start a new `db` transaction call `f` with `db` as argument and
  commit the transaction. If `f` throws any exception the transaction
  gets rolled back."
  [db :- Database f & [opts]]
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

(defn print-explain
  "Print the execution plan of `query`."
  [query]
  (let [db (dissoc (-> query sql/ast :db) :explain?)]
    (doseq [row @(sql/explain db query)]
      (println (get row (keyword "query plan"))))))

(defn execute-sql-query
  "Execute a SQL query."
  [db sql & [opts]]
  (with-connection [db db]
    (prn "EXEC QUERY")
    (try (driver/-fetch (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL query."
                           {:sql sql :opts opts} e))))))

(defn execute-sql-statement
  "Execute a SQL statement."
  [db sql & [opts]]
  (with-connection [db db]
    (prn "EXEC STATEMENT")
    (try (driver/-execute (:driver db) sql opts)
         (catch Exception e
           (throw (ex-info "Can't execute SQL statement."
                           {:sql sql :opts opts} e))))))

(s/defn execute
  "Execute `stmt` against a database."
  [stmt & [opts]]
  (let [{:keys [db] :as ast} (sql/ast stmt)]
    (let [sql (sql/sql ast)]
      (when (:explain? db)
        (prn sql)
        (print-explain stmt))
      (prn sql)
      (prn (:op ast))
      (case (:op ast)
        :create-table
        (if (seq (rest sql))
          ;; TODO: sql-str only works with PostgreSQL driver
          (execute-sql-statement db (sql-str stmt) opts)
          (execute-sql-statement db sql opts))
        :delete
        (if (:returning ast)
          (execute-sql-query db sql opts)
          (execute-sql-statement db sql opts))
        :except
        (execute-sql-query db sql opts)
        :explain
        (execute-sql-query db sql opts)
        :insert
        (if (:returning ast)
          (execute-sql-query db sql opts)
          (execute-sql-statement db sql opts))
        :intersect
        (execute-sql-query db sql opts)
        :select
        (execute-sql-query db sql opts)
        :union
        (execute-sql-query db sql opts)
        :update
        (if (:returning ast)
          (execute-sql-query db sql opts)
          (execute-sql-statement db sql opts))
        :values
        (execute-sql-query db sql opts)
        (execute-sql-statement db sql opts)))))
