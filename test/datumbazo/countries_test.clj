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
      (is (= (map :name rows)
             ["Indonesia" "Spain"])))))

(deftest test-by-id
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-id db (:id country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-iso-3166-1-alpha-2
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-iso-3166-1-alpha-2 db (:iso-3166-1-alpha-2 country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-iso-3166-1-alpha-3
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-iso-3166-1-alpha-3 db (:iso-3166-1-alpha-3 country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-iso-3166-1-numeric
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          row (countries/by-iso-3166-1-numeric db (:iso-3166-1-numeric country))]
      (is (country? row))
      (is (= row country)))))

(deftest test-by-created-at
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          rows (countries/by-created-at db (:created-at country))]
      (is (not-empty rows))
      (is (every? country? rows))
      (is (every? #(= % (:created-at country))
                  (map :created-at rows))))))

(deftest test-by-updated-at
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          rows (countries/by-updated-at db (:updated-at country))]
      (is (not-empty rows))
      (is (every? country? rows))
      (is (every? #(= % (:updated-at country))
                  (map :updated-at rows))))))

(deftest test-by-name
  (with-backends [db]
    (let [row (countries/by-name db "Spain")]
      (is (country? row))
      (is (= (dissoc row :geometry)
             {:updated-at #inst "2016-02-24T12:21:43.575-00:00",
              :iso-3166-1-alpha-3 "esp",
              :name "Spain",
              :iso-3166-1-alpha-2 "es",
              :id 2,
              :iso-3166-1-numeric 724,
              :geonames-id 2510769,
              :continent-id 12,
              :created-at #inst "2012-12-09T22:31:20.515-00:00"})))))

(deftest test-continent
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          continent (:continent country)]
      (is (continent? continent))
      (is (= continent (continents/by-name db "Europe"))))))

(deftest test-delete!
  (with-backends [db]
    ;; (is (empty? (countries/delete! db [])))
    (let [country (countries/by-name db "Spain")
          deleted (countries/delete! db country)]
      (is (nil? (countries/by-name db (:name country))))
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
      (is (pos? (:id country)))
      (is (= (:name country) "Gondwana"))
      (is (= (:iso-3166-1-alpha-2 country) "gd"))
      (is (= (:iso-3166-1-alpha-3 country) "gnd"))
      (is (= (:iso-3166-1-numeric country) 2))
      (is (instance? Date (:created-at country)))
      (is (instance? Date (:updated-at country))))))

(deftest test-sample
  (is (every? map? (gen/sample (s/gen ::countries/countries)))))

(deftest test-update!
  (with-backends [db]
    (let [country (countries/by-name db "Spain")
          updated (countries/update! db (assoc country :name "SPAIN")) ]
      (is (= (:id updated) (:id country)))
      (is (= (:name updated) "SPAIN"))
      (is (instance? Date (:created-at updated)))
      (is (instance? Date (:updated-at updated))))))

(deftest test-truncate!
  (with-backends [db]
    (countries/truncate! db)
    (is (empty? (countries/all db)))))
