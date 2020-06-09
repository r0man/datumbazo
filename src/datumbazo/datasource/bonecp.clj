(ns datumbazo.datasource.bonecp
  (:require [datumbazo.datasource :refer [datasource]]
            [datumbazo.vendor :refer [jdbc-url]])
  (:import [com.jolbox.bonecp BoneCPConfig BoneCPDataSource]))

(defmethod datasource :bonecp [db]
  (let [config (BoneCPConfig.)]
    (.setJdbcUrl config (jdbc-url db))
    (.setUsername config (:username db))
    (.setPassword config (:password db))
    (BoneCPDataSource. config)))
