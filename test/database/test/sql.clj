(ns database.test.sql
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.sql
        database.sql.compiler))

(deftest test-drop-table
  (is (= ["DROP TABLE continents"]
         (sql (drop-table :continents))))
  (is (= ["DROP TABLE IF EXISTS continents RESTRICT"]
         (sql (drop-table
               :continents
               (if-exists true)
               (restrict true))))))

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
