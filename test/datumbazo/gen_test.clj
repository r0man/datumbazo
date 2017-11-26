(ns datumbazo.gen-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [datumbazo.core :as sql]
            [datumbazo.gen :as gens]
            [datumbazo.test :refer :all]))

(deftest test-column-integer
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :id :integer :not-null? true))
    (is (->> (gens/column db :my-table.id)
             (gen/sample)
             (every? int?)))))

(deftest test-column-nilable
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :id :integer))
    (is (->> (gens/column db :my-table.id)
             (gen/sample)
             (every? #(or (nil? %) (int? %)))))))

(deftest test-column-text
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :name :text :not-null? true))
    (is (->> (gens/column db :my-table.name)
             (gen/sample)
             (every? string?)))))

(deftest test-row
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :id :serial :primary-key? true)
       (sql/column :name :text :unique? true))
    (is (->> (gens/row db :my-table)
             (gen/sample)
             (every? map?)))))

(deftest test-insert-primary-key
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :id :serial :primary-key? true))
    (let [rows (gen/sample (gens/row db :my-table) 1000)]
      (is (= @(sql/insert db :my-table []
                (sql/values rows)
                (sql/returning :*))
             rows)))))

(deftest test-insert-unique-key
  (with-backends [db]
    @(sql/create-table db :my-table
       (sql/column :id :serial :unique? true))
    (let [rows (gen/sample (gens/row db :my-table) 1000)]
      (is (= @(sql/insert db :my-table []
                (sql/values rows)
                (sql/returning :*))
             rows)))))
