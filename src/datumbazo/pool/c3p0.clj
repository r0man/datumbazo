(ns datumbazo.pool.c3p0
  (:require [datumbazo.pool.core :refer [db-pool]])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defmethod db-pool :c3p0 [db-spec & [opts]]
  (let [{:keys [subprotocol subname user password]} db-spec
        {:keys [acquire-retry-attempts initial-pool-size initial-pool-size
                max-idle-time max-idle-time-excess-connections max-pool-size
                min-pool-size]} opts
        datasource (ComboPooledDataSource.)]
    (.setJdbcUrl datasource (str "jdbc:" subprotocol ":" subname))
    (.setUser datasource user)
    (.setPassword datasource password)
    (when acquire-retry-attempts
      (.setAcquireRetryAttempts datasource acquire-retry-attempts))
    (when initial-pool-size
      (.setInitialPoolSize datasource initial-pool-size))
    (when max-idle-time
      (.setMaxIdleTime datasource max-idle-time))
    (when max-idle-time-excess-connections
      (.setMaxIdleTimeExcessConnections datasource max-idle-time-excess-connections))
    (when max-pool-size
      (.setMaxPoolSize datasource max-pool-size))
    (when min-pool-size
      (.setMinPoolSize datasource min-pool-size))
    datasource))
