(ns datumbazo.pool.c3p0
  (:require [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.vendor :refer [jdbc-url]])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defmethod db-pool :c3p0 [db & [opts]]
  (let [ds (ComboPooledDataSource.)]
    (.setJdbcUrl ds (jdbc-url db))
    (.setUser ds (:username db))
    (.setPassword ds (:password db))
    (some->> (:acquire-retry-attempts db) (.setAcquireRetryAttempts ds))
    (some->> (:initial-pool-size db) (.setInitialPoolSize ds))
    (some->> (:max-idle-time db) (.setMaxIdleTime ds))
    (some->> (:max-idle-time-excess-connections db) (.setMaxIdleTimeExcessConnections ds))
    (some->> (:max-pool-size db) (.setMaxPoolSize ds))
    (some->> (:min-pool-size db) (.setMinPoolSize ds))
    ds))
