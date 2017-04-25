(ns datumbazo.pool.hikaricp
  (:require [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.vendor :refer [jdbc-url]])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defmethod db-pool :hikaricp [db & [opts]]
  (let [config (HikariConfig.)]
    (.setJdbcUrl config (jdbc-url db))
    (.setUsername config (:username db))
    (.setPassword config (:password db))
    (some->> (:connection-timeout db) (.setConnectionTimeout config))
    (some->> (:idle-timeout db) (.setIdleTimeout config))
    (some->> (:max-lifetime db) (.setMaxLifetime config))
    (some->> (:maximum-pool-size db) (.setMaximumPoolSize config))
    (some->> (:minimum-idle db) (.setMinimumIdle config))
    (HikariDataSource. config)))
