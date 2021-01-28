(ns datumbazo.datasource)

(defn- datasource-namespace
  "Return the namespace of the `db` connection pool."
  [db]
  (symbol (str "datumbazo.datasource." (name (:pool db)))))

(defmulti datasource
  "Return a database connection pool for `db`."
  (fn [db]
    (require (datasource-namespace db))
    (:pool db)))

(defmethod datasource :default [db & [opts]]
  (throw (ex-info (str "Unsupported connection pool: " (:pool db))
                  (into {} db))))

(defn start-datasource
  "Start a connection pool and assoc it onto the :driver of `db`."
  [db]
  (if-let [pool (datasource db)]
    (assoc db :datasource pool)
    db))

(defn stop-datasource
  "Stop the connection pool and dissoc it from the :driver of `db`."
  [db]
  (some-> db :datasource .close)
  (assoc db :datasource nil))
