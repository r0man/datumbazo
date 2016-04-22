(ns datumbazo.pool.bonecp
  (:require [datumbazo.pool.core :refer [db-pool]])
  (:import [com.jolbox.bonecp BoneCPConfig BoneCPDataSource]))

(defmethod db-pool :bonecp [db-spec & [opts]]
  (let [{:keys [subprotocol subname user password]} db-spec
        config (BoneCPConfig.)]
    (.setJdbcUrl config (str "jdbc:" subprotocol ":" subname))
    (.setUsername config user)
    (.setPassword config password)
    (BoneCPDataSource. config)))
