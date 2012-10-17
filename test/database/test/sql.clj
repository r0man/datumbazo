(ns database.test.sql
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.sql))

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

(deftest test-deftable
  (deftable continents
    "The continents database table."
    :continents
    (column :id :serial)
    (column :name :text :not-null? true :unique? true)
    (column :code :varchar :length 2 :not-null? true :unique? true)
    (column :geometry :geometry)
    (column :freebase-guid :text :unique? true)
    (column :geonames-id :integer :unique? true)
    (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
    (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))
  (is (= :table (:op continents)))
  (is (nil? (:schema continents)))
  (is (= :continents (:name continents))))

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

(deftest test-parse-expr
  (are [expr expected]
       (is (= expected (parse-expr expr)))
       nil
       {:op :nil}
       1
       {:op :number :form 1}
       1.2
       {:op :number :form 1.2}
       "Europe"
       {:op :string :form "Europe"}
       :continents.id
       {:op :keyword :form :continents.id}
       '(greatest 1 2)
       {:op :fn :form 'greatest :children [{:op :number :form 1} {:op :number :form 2}]}
       '(max :continents.created-at)
       {:op :fn :form 'max :children [{:op :keyword :form :continents.created-at}]}
       `(max :continents.created-at)
       {:op :fn :form `max :children [{:op :keyword :form :continents.created-at}]}
       '(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
       {:op :fn :form 'ST_AsText :children [{:op :fn :form 'ST_Centroid :children [{:op :string :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}))

(deftest test-select
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (select 1)
       ["SELECT 1"]
       (select [1 2 3])
       ["SELECT 1, 2, 3"]
       (select [] (from :continents))
       ["SELECT * FROM continents"]
       (select :id (from :continents))
       ["SELECT id FROM continents"]
       (select [:id :name] (from :continents))
       ["SELECT id, name FROM continents"]
       (select ['(greatest 1 2) '(lower "X")])
       ["SELECT greatest(1, 2), lower(?)" "X"]))

(deftest test-table
  (let [t (table :continents)]
    (is (= :table (:op t)))
    (is (nil? (:schema t)))
    (is (= :continents (:name t)))
    (is (= t (table "continents"))))
  (let [t (table :public.continents)]
    (is (= :table (:op t)))
    (is (= :public (:schema t)))
    (is (= :continents (:name t)))
    (is (= t (table "public.continents"))))
  (let [t (table
           :public.continents
           (column :id :serial)
           (column :name :text :not-null? true :unique? true)
           (column :code :varchar :length 2 :not-null? true :unique? true)
           (column :geometry :geometry)
           (column :freebase-guid :text :unique? true)
           (column :geonames-id :integer :unique? true)
           (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
           (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))]
    (is (= :table (:op t)))
    (is (= :public (:schema t)))
    (is (= :continents (:name t)))))

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
