(ns datumbazo.driver.core
  (:require [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [sqlingvo.core :as sql :refer [ast sql]])
  (:import [java.sql Connection Savepoint]))

(defprotocol Connectable
  (-connect [driver db opts])
  (-connection [driver db])
  (-disconnect [driver db]))

(defprotocol Executeable
  (-execute-all [driver db sql opts])
  (-execute-one [driver db sql opts]))

(defprotocol Preparable
  (-prepare [driver db sql opts]))

(defprotocol Transactable
  (-transact [driver db f opts]))

(defmulti driver
  "Find the driver for `db`."
  (fn [db & [opts]] (-> db :backend keyword)))

(defn row-count
  "Normalize into a record, with the count of affected rows."
  [result]
  [{:count
    (if (sequential? result)
      (first result)
      result)}])

(defn savepoint?
  "Return true if `x` is a java.sql.Savepoint, otherwise false."
  [x]
  (instance? Savepoint x))

(defn load-drivers
  "Load the driver namespaces."
  []
  (try
    (doseq [ns '[datumbazo.driver.clojure.java.jdbc
                 datumbazo.driver.jdbc.core
                 datumbazo.driver.next.jdbc]]
      (try (require ns)
           (catch Exception _)))))

(defn ^Connection connection
  "Return the current connection to `db`."
  [db]
  (-connection (:driver db) db))

(s/fdef connection
  :args (s/cat :db sql/db?))

(defn connected?
  "Returns true if `db` is connected, otherwise false."
  [db]
  (some? (connection db)))

(s/fdef connected?
  :args (s/cat :db sql/db?))

(defn connect
  "Connect to `db` using `opts`."
  [db & [opts]]
  {:pre [(not (connected? db))]}
  (update db :driver -connect db opts))

(s/fdef connect
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn disconnect
  "Disconnect from `db`."
  [db & [opts]]
  {:pre [(connected? db)]}
  (update db :driver -disconnect db))

(s/fdef disconnect
  :args (s/cat :db sql/db? :opts (s/? (s/nilable map?))))

(defn prepare
  "Return a prepared statement for `sql`."
  [db sql & [opts]]
  {:pre [(connected? db)]}
  (-prepare (:driver db) db sql opts))

(s/fdef prepare
  :args (s/cat :db sql/db? :sql any? :opts (s/? (s/nilable map?))))

(defn transact
  "Run `f` within a database transaction on `db`."
  [db f & [opts]]
  {:pre [(connected? db)]}
  (-transact (:driver db) db f opts))

(s/fdef transact
  :args (s/cat :db sql/db? :f ifn? :opts (s/? (s/nilable map?))))

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
  (transact db f opts))

(s/fdef with-transaction*
  :args (s/cat :db sql/db? :f ifn? :opts (s/? (s/nilable map?))))

(defmacro with-transaction
  "Start a new `db` transaction, bind `db` to `db-sym` and evaluate
  `body` within the transaction."
  [[db-sym db & [opts]] & body]
  `(with-connection [db# ~db]
     (with-transaction* db# (fn [~db-sym] ~@body) ~opts)))

(defmacro with-rollback
  "Start a transaction, evaluate `body` and rollback."
  [[db-sym db] & body]
  `(with-transaction [~db-sym ~db {:rollback-only true}]
     ~@body))

(defn commit!
  "Commit the current database connection."
  [db]
  {:pre [(connected? db)]}
  (.commit (connection db)))

(s/fdef commit!
  :args (s/cat :db sql/db?))

(defn auto-commit?
  "Returns true if the current database connection is in auto commit
  mode, otherwise false."
  [db]
  {:pre [(connected? db)]}
  (.getAutoCommit (connection db)))

(s/fdef auto-commit?
  :args (s/cat :db sql/db?))

(defn set-auto-commit! [db auto-commit]
  {:pre [(connected? db)]}
  (.setAutoCommit (connection db) auto-commit))

(s/fdef set-auto-commit!
  :args (s/cat :db sql/db? :auto-commit boolean?))

(defn rollback! [db & [savepoint]]
  {:pre [(connected? db)]}
  (if savepoint
    (.rollback (connection db) savepoint)
    (.rollback (connection db))))

(s/fdef rollback!
  :args (s/cat :db sql/db? :savepoint (s/? (s/nilable savepoint?))))

(defn ^Savepoint set-savepoint!
  "Set a savepoint on the current connection."
  [db & [name]]
  {:pre [(connected? db)]}
  (if name
    (.setSavepoint (connection db) name)
    (.setSavepoint (connection db))))

(s/fdef set-savepoint!
  :args (s/cat :db sql/db? :name (s/? (s/nilable string?))))

(defn release-savepoint!
  [db savepoint]
  {:pre [(connected? db)]}
  (.releaseSavepoint (connection db) savepoint))

(s/fdef release-savepoint!
  :args (s/cat :db sql/db? :savepoint savepoint?))

(defn with-savepoint*
  [db f & [opts]]
  (with-connection [db db]
    (let [auto-commit (auto-commit? db)]
      (set-auto-commit! db false)
      (let [savepoint (set-savepoint! db)]
        (try (f (assoc-in db [:driver :savepoint] savepoint))
             (catch Exception e
               (rollback! db savepoint)
               (throw e))
             (finally
               (release-savepoint! db savepoint)
               (set-auto-commit! db auto-commit)))))))

(s/fdef with-savepoint*
  :args (s/cat :db sql/db? :f ifn? :opts (s/? (s/nilable map?))))

(defmacro with-savepoint
  "Start a transaction, evaluate `body` and rollback."
  [[db-sym db opts] & body]
  `(with-savepoint* ~db (fn [~db-sym] ~@body) ~opts))

(defn sql-str
  "Prepare `stmt` using the database and return the raw SQL as a string."
  [stmt]
  (let [ast (ast stmt)]
    (with-connection [db (:db ast)]
      (with-open [stmt (prepare db (sql ast))]
        (if (.startsWith (str stmt) (str/replace (first (sql ast)) #"\?.*" ""))
          (str stmt)
          (throw (UnsupportedOperationException.
                  "Sorry, sql-str not supported by SQL driver.")))))))

(defn- error-message [message [sql & args] exception]
  (str message "\n\n"
       (ex-message exception) "\n\n"
       sql "\n\n"
       (with-out-str (pprint args))
       "\n"))

(defn execute-sql-query
  "Execute a SQL query."
  [db sql & [opts]]
  (with-connection [db db]
    (try (-execute-all (:driver db) db sql opts)
         (catch Exception e
           (throw (ex-info (error-message "Can't execute SQL query." sql e)
                           {:type :datumbazo/execute-sql-query-error
                            :db db
                            :sql sql
                            :opts opts} e))))))

(s/fdef execute-sql-query
  :args (s/cat :db sql/db? :sql any? :opts (s/? (s/nilable map?))))

(defn execute-sql-statement
  "Execute a SQL statement."
  [db sql & [opts]]
  (with-connection [db db]
    (try (-execute-one (:driver db) db sql opts)
         (catch Exception e
           (throw (ex-info (error-message "Can't execute SQL statement." sql e)
                           {:type :datumbazo/execute-sql-statement-error
                            :db db
                            :sql sql
                            :opts opts} e))))))

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
    (let [sql (sql ast)
          exec-fn (execute-fn ast)]
      (with-meta
        (case (:op ast)
          :create-table
          (if (seq (rest sql))
            ;; TODO: sql-str only works with PostgreSQL driver
            (exec-fn db [(sql-str stmt)] opts)
            (exec-fn db sql opts))
          (exec-fn db sql opts))
        {:datumbazo/stmt stmt
         :datumbazo/opts opts}))))
