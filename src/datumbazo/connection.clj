(ns datumbazo.connection
  (:require [datumbazo.driver.core :as driver]
            [schema.core :as s]
            [sqlingvo.core :refer [ast sql]])
  (:import java.sql.Connection
           java.sql.PreparedStatement
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

(s/defn execute
  "Execute `stmt` against a database."
  [stmt & [opts]]
  (let [{:keys [db] :as ast} (ast stmt)
        driver (:driver db)
        sql (sql ast)]
    (assert (connected? db))
    (case (:op ast)
      :delete
      (if (:returning ast)
        (driver/-fetch driver sql opts)
        (driver/-execute driver sql opts))
      :except
      (driver/-fetch driver sql opts)
      :explain
      (driver/-fetch driver sql opts)
      :insert
      (if (:returning ast)
        (driver/-fetch driver sql opts)
        (driver/-execute driver sql opts))
      :intersect
      (driver/-fetch driver sql opts)
      :select
      (driver/-fetch driver sql opts)
      :union
      (driver/-fetch driver sql opts)
      :update
      (if (:returning ast)
        (driver/-fetch driver sql opts)
        (driver/-execute driver sql opts))
      :values
      (driver/-fetch driver sql opts)
      (driver/-execute driver sql opts))))

(s/defn with-connection*
  "Open a database connection, call `f` with the connected `db` as
  argument and close the connection again."
  [db :- Database f & [opts]]
  (let [db (connect db opts)]
    (try (f db)
         (finally (disconnect db)))))

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
