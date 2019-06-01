(ns datumbazo.people
  (:require [datumbazo.core :as sql]
            [datumbazo.table :as t]))

(t/deftable people
  "The people database table."
  (t/table :person)
  (sql/column :name :text :primary-key? true)
  (sql/column :mood :mood))
