(ns datumbazo.driver.core)

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
    (doseq [ns '[datumbazo.driver.clojure
                 datumbazo.driver.funcool]]
      (try (require ns)
           (catch Exception _)))))
