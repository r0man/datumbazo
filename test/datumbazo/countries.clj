(ns datumbazo.countries
  (:require [datumbazo.associations :as a]
            [datumbazo.core :as sql]
            [datumbazo.record :refer [select-columns]]))

(sql/deftable ::countries
  "The countries database table."
  (sql/column :country/id :serial :primary-key? true)
  (sql/column :country/continent-id :integer :references :continents/id)
  (sql/column :country/name :citext :not-null? true :unique? true)
  (sql/column :country/geometry :geometry)
  (sql/column :country/geonames-id :integer :unique? true)
  (sql/column :country/iso-3166-1-alpha-2 :varchar :size 2 :not-null? true :unique? true)
  (sql/column :country/iso-3166-1-alpha-3 :varchar :size 3 :not-null? true :unique? true)
  (sql/column :country/iso-3166-1-numeric :integer :not-null? true :unique? true)
  (sql/column :country/created-at :timestamp :not-null? true)
  (sql/column :country/updated-at :timestamp :not-null? true)
  (a/belongs-to :country/continent))

(defmethod select-columns Country [class & [opts]]
  [:countries.id
   :countries.continent-id
   :countries.name
   :countries.geometry
   :countries.geonames-id
   (sql/as :countries.iso-3166-1-alpha-2 :iso-3166-1-alpha-2)
   :countries.iso-3166-1-alpha-3
   :countries.iso-3166-1-numeric
   :countries.created-at
   :countries.updated-at])
