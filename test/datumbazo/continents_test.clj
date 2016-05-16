(ns datumbazo.continents-test
  (:require [clojure.test :refer :all]
            [datumbazo.continents :as continents :refer [continent?]]
            [datumbazo.countries :refer [country?]]
            [datumbazo.util :refer [make-instance]]
            [datumbazo.test :refer :all])
  (:import datumbazo.continents.Continent
           java.util.Date))

(def new-continent
  {:name "Gondwana" :code "GD"})

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
              :created-at #inst "2012-10-06T18:22:58.640-00:00"
              :updated-at #inst "2012-10-06T18:22:58.640-00:00"})))))

(deftest test-callbacks-all
  (with-test-db [db db]
    (continents/reset-counters)
    (let [rows (continents/all db)]
      (is (every? #(= (continents/counters-for %)
                      {:after-initialize 1
                       :after-find 1})
                  rows)))))

(deftest test-callbacks-by-name
  (with-test-db [db db]
    (continents/reset-counters)
    (let [continent (continents/by-name db "Europe")]
      (is (= (continents/counters-for continent)
             {:after-initialize 1
              :after-find 1})))))

(deftest test-callbacks-delete!
  (with-test-db [db db]
    (let [continent (continents/by-name db "Europe")]
      (continents/reset-counters)
      (continents/delete! db continent)
      (is (= (continents/counters-for continent)
             {:after-initialize 2
              :before-delete 1
              :after-delete 1})))))

(deftest test-callbacks-insert!
  (with-test-db [db db]
    (continents/reset-counters)
    (let [continent (continents/insert! db new-continent)]
      (is (= (continents/counters-for continent)
             {:after-initialize 1
              :after-create 1}))
      (is (= (continents/counters-for (assoc continent :id nil))
             {:after-initialize 1
              :before-create 1})))))

(deftest test-callbacks-update!
  (with-test-db [db db]
    (let [continent (continents/by-name db "Europe")]
      (continents/reset-counters)
      (continents/update! db continent)
      (is (= (continents/counters-for continent)
             {:after-initialize 2
              :before-update 1
              :after-update 1})))))

(deftest test-callbacks-save!
  (with-test-db [db db]
    (let [continent (continents/by-name db "Europe")]
      (continents/reset-counters)
      (let [continent (continents/save! db new-continent)]
        (is (= (continents/counters-for continent)
               {:after-initialize 1
                :after-save 1}))
        (is (= (continents/counters-for (empty continent))
               {:after-initialize 1
                :before-save 1}))
        (is (= (continents/save! db continent) continent))
        (is (= (continents/counters-for continent)
               {:after-initialize 3
                :after-save 2
                :before-save 1}))
        (is (= (continents/save! db new-continent) continent))
        (is (= (continents/save! db new-continent) continent))))))

(deftest test-has-many-countries
  (with-backends [db]
    (let [continent (continents/by-name db "Asia")
          countries (:countries continent)]
      (is (= (map :name countries) ["Indonesia"]))
      (is (every? country? countries)))))

(deftest test-delete!
  (with-backends [db]
    ;; (is (empty? (continents/delete! db nil)))
    (let [continent (continents/by-name db "Asia")
          deleted (continents/delete! db continent)]
      (is (nil? (continents/by-name db (:name continent))))
      (is (= deleted continent))
      (is (nil? (continents/delete! db continent))))))

(deftest test-insert!
  (with-backends [db]
    (let [continent (continents/insert! db new-continent)]
      (is (continent? continent))
      (is (pos? (:id continent)))
      (is (= (:name continent) "Gondwana"))
      (is (= (:code continent) "GD"))
      (is (instance? Date (:created-at continent)))
      (is (instance? Date (:updated-at continent))))))

(deftest test-update!
  (with-backends [db]
    (let [continent (continents/by-name db "Asia")
          updated (continents/update! db (assoc continent :name "ASIA")) ]
      (is (= (:id updated) (:id continent)))
      (is (= (:name updated) "ASIA"))
      (is (= (:code updated) (:code continent)))
      (is (instance? Date (:created-at updated)))
      (is (instance? Date (:updated-at updated))))))

(deftest test-save!
  (with-backends [db]
    (let [continent (continents/save! db new-continent)]
      (is (continent? continent))
      (is (pos? (:id continent)))
      (is (= (:name continent) "Gondwana"))
      (is (= (:code continent) "GD"))
      (is (instance? Date (:created-at continent)))
      (is (instance? Date (:updated-at continent)))
      (let [continent (continents/save! db continent)]
        (is (continent? continent))
        (is (pos? (:id continent)))
        (is (= (:name continent) "Gondwana"))
        (is (= (:code continent) "GD"))
        (is (instance? Date (:created-at continent)))
        (is (instance? Date (:updated-at continent)))))))

(deftest test-truncate!
  (with-backends [db]
    (try (continents/truncate! db)
         (assert false)
         (catch Exception _)))
  (with-backends [db]
    (continents/truncate! db {:cascade true})
    (is (empty? (continents/all db)))))
