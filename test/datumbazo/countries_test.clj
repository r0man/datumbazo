(ns datumbazo.countries-test
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.test :refer :all]
            [datumbazo.util :refer [make-instance]]
            [datumbazo.countries :as countries :refer [country?]]
            [datumbazo.continents :refer [continent?]]
            [datumbazo.test :refer :all]
            [datumbazo.continents :as continents])
  (:import datumbazo.countries.Country
           java.util.Date))

(deftest test-country?
  (is (country? (make-instance Country {:a 1})))
  (is (not (country? {}))))

(deftest test-all
  (with-backends [db]
    (let [rows (countries/all db)]
      (is (not-empty rows))
      (is (every? country? rows))
      (is (= (map :country/name rows)
             ["Indonesia" "Spain"])))))

(deftest test-by-id
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-id db (:country/id country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-iso-3166-1-alpha-2
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-iso-3166-1-alpha-2 db (:country/iso-3166-1-alpha-2 country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-iso-3166-1-alpha-3
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-iso-3166-1-alpha-3 db (:country/iso-3166-1-alpha-3 country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-iso-3166-1-numeric
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-iso-3166-1-numeric db (:country/iso-3166-1-numeric country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-created-at
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          rows (countries/by-created-at db (:country/created-at country))]
      (is (not-empty rows))
      (is (every? country? rows))
      (is (every? #(= % (:created-at country))
                  (map :created-at rows))))))

(deftest test-by-updated-at
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          rows (countries/by-updated-at db (:country/updated-at country))]
      (is (not-empty rows))
      (is (every? country? rows))
      (is (every? #(= % (:country/updated-at country))
                  (map :country/updated-at rows))))))

(deftest test-by-name
  (with-backends [db]
    (let [row (countries/by-name db "Spain")]
      (is (country? row))
      (is (= (dissoc row :country/geometry)
             {:country/updated-at #inst "2016-02-24T12:21:43.575-00:00",
              :country/iso-3166-1-alpha-3 "esp",
              :country/name "Spain",
              :country/iso-3166-1-alpha-2 "es",
              :country/id 2,
              :country/iso-3166-1-numeric 724,
              :country/geonames-id 2510769,
              :country/continent-id 12,
              :country/created-at #inst "2012-12-09T22:31:20.515-00:00"})))))

(deftest test-continent
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          continent (:country/continent country)]
      (is (continent? continent))
      (is (= continent (continents/by-name db "Europe"))))))

(deftest test-delete!
  (with-backends [db]
    ;; (is (empty? (countries/delete! db [])))
    (let [country (countries/by-name db "Spain")
          deleted (countries/delete! db country)]
      (is (nil? (countries/by-name db (:country/name country))))
      (is (= deleted country))
      (is (nil? (countries/delete! db country))))))

(deftest test-insert!
  (with-backends [db]
    (let [attrs {:name "Gondwana"
                 :geonames-id 1
                 :iso-3166-1-alpha-2 "gd"
                 :iso-3166-1-alpha-3 "gnd"
                 :iso-3166-1-numeric 2}
          country (countries/insert! db attrs)]
      (is (country? country))
      (is (pos? (:country/id country)))
      (is (= (:country/name country) "Gondwana"))
      (is (= (:country/iso-3166-1-alpha-2 country) "gd"))
      (is (= (:country/iso-3166-1-alpha-3 country) "gnd"))
      (is (= (:country/iso-3166-1-numeric country) 2))
      (is (instance? Date (:country/created-at country)))
      (is (instance? Date (:country/updated-at country))))))

(deftest test-sample
  (doseq [country (gen/sample (s/gen ::countries/countries))]
    (is (map? country))
    (is (string? (:country/name country)))
    (is (or (integer? (:country/continent-id country))
            (nil? (:country/continent-id country))))
    ;; TODO: geometry
    (is (or (integer? (:country/geonames-id country))
            (nil? (:country/geonames-id country))))
    (is (= (count (:country/iso-3166-1-alpha-2 country)) 2))
    (is (= (count (:country/iso-3166-1-alpha-3 country)) 3))
    (is (inst? (:country/created-at country)))
    (is (inst? (:country/updated-at country)))))

(deftest test-update!
  (with-backends [db]
    (let [country (dissoc (countries/by-name db "Spain") :country/geometry)
          updated (countries/update! db (assoc country :country/name "SPAIN")) ]
      (is (= (:country/id updated) (:country/id country)))
      (is (= (:country/name updated) "SPAIN"))
      (is (instance? Date (:country/created-at updated)))
      (is (instance? Date (:country/updated-at updated))))))

(deftest test-truncate!
  (with-backends [db]
    (countries/truncate! db)
    (is (empty? (countries/all db)))))
