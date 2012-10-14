(ns database.test.sql
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.sql
        database.sql.compiler))

(deftest test-column
  (let [t (table
           :public.continents
           (column :id :serial :primary-key? true)
           (column :name :text :not-null? true :unique? true))]
    (is (= :public (:schema t)))
    (is (= :continents (:name t)))
    (let [c (get-in t [:column :id])]
      (is (= :public (:schema c)))
      (is (= :continents (:table c)))
      (is (= :id (:name c)))
      (is (= :serial (:type c)))
      (is (= true (:primary-key? c))))
    (let [c (get-in t [:column :name])]
      (is (= :public (:schema c)))
      (is (= :continents (:table c)))
      (is (= :name (:name c)))
      (is (= :text (:type c)))
      (is (= true (:not-null? c)))
      (is (= true (:unique? c))))))

(deftest test-drop-table
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (drop-table :continents)
       ["DROP TABLE continents"]
       (drop-table [:continents :countries])
       ["DROP TABLE continents, countries"]
       (drop-table
        :continents
        (if-exists true)
        (restrict true))
       ["DROP TABLE IF EXISTS continents RESTRICT"]))

(deftest test-select
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (select 1)
       ["SELECT 1"]
       (select [] (from :continents))
       ["SELECT * FROM continents"]
       (select [:id :name] (from :continents))
       ["SELECT id, name FROM continents"]))

(deftest test-table
  (let [t (table :continents)]
    (is (nil? (:schema t)))
    (is (= :continents (:name t)))
    (is (= t (table "continents")))
    (is (= t (table t))))
  (let [t (table :public.continents)]
    (is (= :public (:schema t)))
    (is (= :continents (:name t)))
    (is (= t (table "public.continents")))))

(deftest test-truncate-table
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (truncate-table :continents)
       ["TRUNCATE TABLE continents"]
       (truncate-table [:continents :countries])
       ["TRUNCATE TABLE continents, countries"]
       (truncate-table
        :continents
        (cascade true)
        (continue-identity true)
        (restart-identity true)
        (restrict true))
       ["TRUNCATE TABLE continents RESTART IDENTITY CONTINUE IDENTITY CASCADE RESTRICT"]))

;; (select 1 (from :continents))