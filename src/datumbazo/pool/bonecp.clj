(ns datumbazo.pool.bonecp
  (:require [datumbazo.pool.core :refer [db-pool]]
            [datumbazo.vendor :refer [jdbc-url]])
  (:import [com.jolbox.bonecp BoneCPConfig BoneCPDataSource]))

(defmethod db-pool :bonecp [db & [opts]]
  (let [config (BoneCPConfig.)]
    (.setJdbcUrl config (jdbc-url db))
    (.setUsername config (:username db))
    (.setPassword config (:password db))
    (BoneCPDataSource. config)))
