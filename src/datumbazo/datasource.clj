(ns datumbazo.datasource)

(defmulti datasource
  "Return a database connection pool for `db`."
  (fn [db] (:pool db)))

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

(defn load-connection-pools
  "Load connection datasource support."
  []
  (doseq [ns '[datumbazo.datasource.bonecp
               datumbazo.datasource.c3p0
               datumbazo.datasource.hikaricp]]
    (try (require ns) (catch Exception e))))
