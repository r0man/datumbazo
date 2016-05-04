(ns datumbazo.continents-test
  (:require [clojure.test :refer :all]
            [datumbazo.continents :as continents :refer [continent?]]
            [datumbazo.countries :refer [country?]]
            [datumbazo.util :refer [make-instance]]
            [datumbazo.test :refer :all])
  (:import datumbazo.continents.Continent
           java.util.Date))

(deftest test-continent?
  (is (continent? (make-instance Continent {:a 1})))
  (is (not (continent? {}))))

(deftest test-all
  (with-backends [db]
    (let [rows (continents/all db)]
      (is (not-empty rows))
      (is (every? continent? rows))
      (is (= (map :id rows)
             [9 10 11 12 13 14 15])))))

(deftest test-by-id
  (with-backends [db]
    (let [continent (continents/by-name db "Europe")
          row (continents/by-id db (:id continent))]
      (is (continent? row))
      (is (= row continent)))))

(deftest test-by-created-at
  (with-backends [db]
    (let [continent (continents/by-name db "Europe")
          rows (continents/by-created-at db (:created-at continent))]
      (is (not-empty rows))
      (is (every? continent? rows))
      (is (every? #(= % (:created-at continent))
                  (map :created-at rows))))))

(deftest test-by-updated-at
  (with-backends [db]
    (let [continent (continents/by-name db "Europe")
          rows (continents/by-updated-at db (:updated-at continent))]
      (is (not-empty rows))
      (is (every? continent? rows))
      (is (every? #(= % (:updated-at continent))
                  (map :updated-at rows))))))

(deftest test-by-name
  (with-backends [db]
    (let [row (continents/by-name db "Europe")]
      (is (continent? row))
      (is (= row
             {:id 12
              :name "Europe"
              :code "EU"
              :geometry nil
              :geonames-id 6255148
              :created-at #inst "2012-10-06T18:22:58.640-00:00"
              :updated-at #inst "2012-10-06T18:22:58.640-00:00"})))))

(deftest test-has-many-countries
  (with-backends [db]
    (let [continent (continents/by-name db "Asia")
          countries (:countries continent)]
      (is (= (map :name countries) ["Indonesia"]))
      (is (every? country? countries)))))

(deftest test-delete!
  (with-backends [db]
    (is (empty? (continents/delete! db [])))
    (let [continent (continents/by-name db "Asia")
          [deleted] (continents/delete! db [continent])]
      (is (nil? (continents/by-name db (:name continent))))
      (is (= deleted continent))
      (is (= (continents/delete! db [continent]) [])))))

(deftest test-insert!
  (with-backends [db]
    (let [[continent] (continents/insert! db [{:name "Gondwana" :code "GD"}])]
      (is (continent? continent))
      (is (pos? (:id continent)))
      (is (= (:name continent) "Gondwana"))
      (is (= (:code continent) "GD"))
      (is (instance? Date (:created-at continent)))
      (is (instance? Date (:updated-at continent))))))

(deftest test-update!
  (with-backends [db]
    (let [continent (continents/by-name db "Asia")
          [updated] (continents/update! db [(assoc continent :name "ASIA")]) ]
      (is (= (:id updated) (:id continent)))
      (is (= (:name updated) "ASIA"))
      (is (= (:code updated) (:code continent)))
      (is (instance? Date (:created-at updated)))
      (is (instance? Date (:updated-at updated))))))

(deftest test-truncate!
  (with-backends [db]
    (try (continents/truncate! db)
         (assert false)
         (catch Exception _)))
  (with-backends [db]
    (continents/truncate! db {:cascade true})
    (is (empty? (continents/all db)))))
