(ns database.test.examples
  (:use [clojure.string :only (lower-case)]
        clojure.test
        database.core
        database.tables
        database.test))

(deftable test
  [[:id :serial :primary-key true]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(deftable photo-thumbnails
  [[:id :serial :primary-key true]
   [:title :text]
   [:description :text]
   [:taken-at :timestamp-with-time-zone]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(deftable continents
  [[:id :serial :primary-key true]
   [:name :text :not-null? true :unique? true]
   [:iso-3166-1-alpha-2 :varchar :length 2 :unique? true :serialize #'lower-case]
   [:location [:point-2d]]
   [:geometry [:multipolygon-2d]]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(deftable languages
  [[:id :serial :primary-key true]
   [:name :text :unique? true :not-null? true]
   [:family :text :not-null? true]
   [:iso-639-1 :varchar :length 2 :unique? true :not-null? true :serialize #'lower-case]
   [:iso-639-2 :varchar :length 3 :unique? true :not-null? true :serialize #'lower-case]
   [:created-at :timestamp-with-time-zone :not-null? true :default "now()"]
   [:updated-at :timestamp-with-time-zone :not-null? true :default "now()"]])

(def continents-table (find-table :continents))
(def test-table (find-table :test))
(def photo-thumbnails-table (find-table :photo-thumbnails))

(deftest test-deserialize-language
  (let [language (deserialize-language {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (instance? Language language))
    (is (= "German" (:name language)))
    (is (= "DE" (:iso-639-1 language)))
    (is (= "DEU" (:iso-639-2 language)))))

(deftest test-serialize-language
  (let [language (serialize-language {:name "German" :iso-639-1 "DE" :iso-639-2 "DEU"})]
    (is (map? language))
    (is (not (instance? Language language)))
    (is (= "German" (:name language)))
    (is (= "de" (:iso-639-1 language)))
    (is (= "deu" (:iso-639-2 language)))))

(load-environments)
