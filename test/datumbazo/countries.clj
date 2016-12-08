(ns datumbazo.countries
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.core :refer :all :exclude [deftable]]
            [datumbazo.table :refer [deftable]]))

(deftable countries
  "The countries database table."
  (column :id :serial :primary-key? true)
  (column :continent-id :integer :references :continents/id)
  (column :name :citext :not-null? true :unique? true)
  (column :geometry :geometry)
  (column :geonames-id :integer :unique? true)
  (column :iso-3166-1-alpha-2 :varchar :size 2 :not-null? true :unique? true)
  (column :iso-3166-1-alpha-3 :varchar :size 3 :not-null? true :unique? true)
  (column :iso-3166-1-numeric :integer :not-null? true :unique? true)
  (column :created-at :timestamp :not-null? true)
  (column :updated-at :timestamp :not-null? true)
  (belongs-to :continent))
