(ns datumbazo.continents
  (:require [datumbazo.callbacks :as callback]
            [datumbazo.table :as t]))

(t/deftable ::continents
  "The continents database table."
  (t/column :id :serial :primary-key? true)
  (t/column :name :text :not-null? true :unique? true)
  (t/column :code :varchar :size 2 :not-null? true :unique? true)
  (t/column :created-at :timestamp :not-null? true)
  (t/column :updated-at :timestamp :not-null? true)
  (t/has-many :countries :batch-size 2 :dependent :destroy))

;; Test callback counts

(def ^:dynamic *counters*
  "Callback counters."
  (atom {}))

(defn reset-counters
  "Reset the callback counters."
  []
  (reset! *counters* {}))

(defn counters-for
  "Return the callback counters for `record`."
  [record]
  (get-in @*counters* [(class record) (:id record)]))

(defn count-callback
  "Increment the `callback` counter for `record`."
  [callback record]
  (swap! *counters* update-in [(class record) (:id record) callback] (fnil inc 0))
  record)

(extend-type Continent

  callback/IAfterCreate
  (after-create [continent]
    (count-callback :after-create continent))

  callback/IAfterDelete
  (after-delete [continent]
    (count-callback :after-delete continent))

  callback/IAfterInitialize
  (after-initialize [continent]
    (count-callback :after-initialize continent))

  callback/IAfterFind
  (after-find [continent]
    (count-callback :after-find continent))

  callback/IAfterSave
  (after-save [continent]
    (count-callback :after-save continent))

  callback/IAfterUpdate
  (after-update [continent]
    (count-callback :after-update continent))

  callback/IBeforeCreate
  (before-create [continent]
    (count-callback :before-create continent))

  callback/IBeforeDelete
  (before-delete [continent]
    (count-callback :before-delete continent))

  callback/IBeforeSave
  (before-save [continent]
    (count-callback :before-save continent))

  callback/IBeforeUpdate
  (before-update [continent]
    (count-callback :before-update continent)))
