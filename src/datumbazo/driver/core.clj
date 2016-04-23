(ns datumbazo.driver.core
  (:require [sqlingvo.core :refer [ast sql]]))

(defmulti apply-transaction
  "Apply `f` within a database transaction"
  (fn [db f & [opts]] (:backend db)))

(defmulti begin
  "Begin a `db` transaction."
  (fn [db & [opts]] (:backend db)))

(defmulti commit
  "Commit a `db` transaction."
  (fn [db & [opts]] (:backend db)))

(defmulti close-connection
  "Close the current database connection to `db`."
  (fn [db] (:backend db)))

(defmulti fetch
  "Execute `sql` and return rows."
  (fn [db sql & [opts]] (:backend db)))

(defmulti connection
  "Get the current `db` connection."
  (fn [db & [opts]] (:backend db)))

(defmulti execute
  "Execute `sql` and return the number of affected rows."
  (fn [db sql & [opts]] (:backend db)))

(defmulti open-connection
  "Open a database connection to `db`. Assoc the connection under
  the :connection in `db` and return `db`."
  (fn [db & [opts]] (:backend db)))

(defmulti prepare-statement
  "Return a prepared statement for `sql`."
  (fn [db sql & [opts]] (:backend db)))

(defmulti rollback!
  "Rollback the current `db` transaction."
  (fn [db] (:backend db)))

(defn eval-db
  "Eval the `stmt` against a database."
  [stmt & [opts]]
  (let [{:keys [db] :as ast} (ast stmt)
        sql (sql ast)]
    (case (:op ast)
      :delete
      (if (:returning ast)
        (fetch db sql opts)
        (execute db sql opts))
      :except
      (fetch db sql opts)
      :explain
      (fetch db sql opts)
      :insert
      (if (:returning ast)
        (fetch db sql opts)
        (execute db sql opts))
      :intersect
      (fetch db sql opts)
      :select
      (fetch db sql opts)
      :union
      (fetch db sql opts)
      :update
      (if (:returning ast)
        (fetch db sql opts)
        (execute db sql opts))
      :values
      (fetch db sql opts)
      (execute db sql opts))))

(defn row-count
  "Normalize into a record, with the count of affected rows."
  [result]
  [{:count
    (if (sequential? result)
      (first result)
      result)}])

(defn with-connection*
  "Open a database connection, call `f` with the connected `db` as
  it's first argument and close the connection again. Does not open a
  new connection if `db` is already connected."
  [db f & [opts]]
  (if (connection db)
    (f db)
    (let [db (open-connection db opts)]
      (try (f db)
           (finally (close-connection db))))))

(defmacro with-connection
  "Open a database connection, bind the connected `db` to `db-sym` and
  evaluate `body`."
  [[db-sym db & [opts]] & body]
  `(with-connection* ~db (fn [~db-sym] ~@body) ~opts))

(defmacro with-transaction
  "Start a new transaction on `db` connection, bind it to `db-sym` and
  evaluate `body` within the transaction."
  [[db-sym db & [opts]] & body]
  `(let [f# (fn [db#]
              (let [~db-sym db#
                    result# (do ~@body)]
                (when (:rollback? ~opts)
                  (rollback! db#))
                result#))]
     (apply-transaction ~db f# ~opts)))
