(ns datumbazo.driver.core
  (:require [sqlingvo.core :refer [ast sql]]))

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

(declare connected? connection)

(defn begin
  "Begin a new `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(-begin % opts)))

(defn commit
  "Commit the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(-commit % opts)))

(defn connect
  "Connect to `db` using `opts`."
  [db & [opts]]
  (update db :driver #(-connect % opts)))

(defn connected?
  "Returns true if `db` is connected, otherwise false."
  [db]
  (some? (connection db)))

(defn connection
  "Return the current connection to `db`."
  [db]
  (-connection (:driver db)))

(defn disconnect
  "Disconnect from `db`."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver -disconnect))

(defmulti find-driver
  "Find the driver for `db`."
  (fn [db & [opts]] (:backend db)))

(defn prepare-statement
  "Return a prepared statement for `sql`."
  [db sql & [opts]]
  {:pre [(connected? db)]}
  (-prepare-statement (:driver db) sql opts))

(defn rollback
  "Rollback the current `db` transaction."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver #(-rollback % opts)))

(defn execute
  "Execute `stmt` against a database."
  [stmt & [opts]]
  (let [{:keys [db] :as ast} (ast stmt)
        driver (:driver db)
        sql (sql ast)]
    (case (:op ast)
      :delete
      (if (:returning ast)
        (-fetch driver sql opts)
        (-execute driver sql opts))
      :except
      (-fetch driver sql opts)
      :explain
      (-fetch driver sql opts)
      :insert
      (if (:returning ast)
        (-fetch driver sql opts)
        (-execute driver sql opts))
      :intersect
      (-fetch driver sql opts)
      :select
      (-fetch driver sql opts)
      :union
      (-fetch driver sql opts)
      :update
      (if (:returning ast)
        (-fetch driver sql opts)
        (-execute driver sql opts))
      :values
      (-fetch driver sql opts)
      (-execute driver sql opts))))

(defn row-count
  "Normalize into a record, with the count of affected rows."
  [result]
  [{:count
    (if (sequential? result)
      (first result)
      result)}])

(defn with-connection*
  "Open a database connection, call `f` with the connected `db` as
  argument and close the connection again."
  [db f & [opts]]
  (let [db (connect db opts)]
    (try (f db)
         (finally (disconnect db)))))

(defn with-transaction*
  "Start a new `db` transaction call `f` with `db` as argument and
  commit the transaction. If `f` throws any exception the transaction
  gets rolled back."
  [db f & [opts]]
  (try
    (let [db (begin db opts)
          ret (f db)]
      (commit db opts)
      ret)
    (catch Throwable t
      (rollback db)
      (throw t))))

(defmacro with-connection
  "Open a database connection, bind the connected `db` to `db-sym`,
  evaluate `body` and close the connection again."
  [[db-sym db & [opts]] & body]
  `(with-connection* ~db (fn [~db-sym] ~@body) ~opts))

(defmacro with-transaction
  "Start a new `db` transaction, bind `db` to `db-sym` and evaluate
  `body` within the transaction."
  [[db-sym db & [opts]] & body]
  `(with-transaction* ~db (fn [~db-sym] ~@body) ~opts))
