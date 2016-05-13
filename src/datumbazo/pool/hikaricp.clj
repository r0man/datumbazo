(ns datumbazo.pool.hikaricp
  (:require [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.vendor :refer [jdbc-url]])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defmethod db-pool :hikaricp [db & [opts]]
  (let [{:keys [connection-timeout idle-timeout max-lifetime
                maximum-pool-size minimum-idle]} opts
        config (HikariConfig.)]
    (.setJdbcUrl config (jdbc-url db))
    (.setUsername config (:username db))
    (.setPassword config (:password db))
    (when connection-timeout
      (.setConnectionTimeout config connection-timeout))
    (when idle-timeout
      (.setIdleTimeout config idle-timeout))
    (when max-lifetime
      (.setMaxLifetime config max-lifetime))
    (when maximum-pool-size
      (.setMaximumPoolSize config maximum-pool-size))
    (when minimum-idle
      (.setMinimumIdle config minimum-idle))
    (HikariDataSource. config)))
