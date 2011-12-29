(ns database.test.examples
  (:use database.tables))

(deftable photo-thumbnails
  [[:id :serial :primary-key true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null true :default "now()"]])