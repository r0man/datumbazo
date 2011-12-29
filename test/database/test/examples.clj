(ns database.test.examples
  (:use database.core))

(deftable photo-thumbnails
  [[:id :serial :primary-key true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null true :default "now()"]])

(deftable continents
  [[:id :serial :primary-key true]
   [:name :text :not-null true :unique true]
   [:iso-3166-1-alpha-2 "varchar(2)" :unique true]
   [:location :point-2d :add-fn add-column]
   [:geometry :multipolygon-2d :add-fn add-column]
   [:freebase-guid :text :unique true :not-null true]
   [:geonames-id :integer :unique true :not-null true]
   [:countries :integer :not-null true :default 0]
   [:regions :integer :not-null true :default 0]
   [:spots :integer :not-null true :default 0]
   [:created-at :timestamp-with-time-zone :not-null true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null true :default "now()"]])
