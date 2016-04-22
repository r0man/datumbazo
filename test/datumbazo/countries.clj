(ns datumbazo.countries
  (:refer-clojure :exclude [distinct group-by update])
  (:require [datumbazo.core :refer :all :exclude [deftable]]
            [datumbazo.table :refer [deftable]]))

(deftable countries
  "The countries database table."
  (column :id :serial :primary-key? true)
  (column :continent-id :integer :references :continents/id)
  (column :name :citext :unique? true)
  (column :geometry :geometry)
  (column :geonames-id :integer  :unique? true)
  (column :iso-3166-1-alpha-2 :varchar :length 2 :unique? true)
  (column :iso-3166-1-alpha-3 :varchar :length 3 :unique? true)
  (column :iso-3166-1-numeric :integer :unique? true)
  (column :created-at :timestamp)
  (column :updated-at :timestamp)
  (belongs-to :continent))
