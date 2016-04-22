(ns datumbazo.pool.hikaricp
  (:require [datumbazo.pool.core :refer [db-pool]])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defmethod db-pool :hikaricp [db-spec & [opts]]
  (let [{:keys [subprotocol subname user password]} db-spec
        {:keys [connection-timeout idle-timeout max-lifetime
                maximum-pool-size minimum-idle]} opts
        config (HikariConfig.)]
    (.setJdbcUrl config (str "jdbc:" subprotocol ":" subname))
    (.setUsername config user)
    (.setPassword config password)
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
