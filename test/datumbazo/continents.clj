(ns datumbazo.continents
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.core :refer :all :exclude [deftable]]
            [datumbazo.table :refer [deftable]]
            [datumbazo.callbacks :as callback]))

(deftable continents
  "The continents database table."
  (column :id :serial :primary-key? true)
  (column :name :varchar :unique? true)
  (column :code :varchar :unique? true)
  (column :created-at :timestamp)
  (column :updated-at :timestamp)
  (has-many :countries :batch-size 2 :dependent :destroy))

(comment
  (require '[datumbazo.callbacks :as callback])
  (extend-type Continent
    callback/IAfterInitialize
    (after-initialize [continent db]
      (println "After initialize continent.")
      continent)
    callback/IBeforeCreate
    (before-create [continent db]
      (println "Before create continent.")
      continent)
    callback/IAfterCreate
    (after-create [continent db]
      (println "After create continent.")
      continent)
    callback/IAfterFind
    (after-find [continent db]
      (println "After find continent.")
      (assoc continent :found true))))
