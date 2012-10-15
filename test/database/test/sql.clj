(ns database.test.sql
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.sql
        database.sql.compiler))

(deftest test-expand-sql
  (are [forms expected]
       (is (= expected (expand-sql forms)))
       '(count *)
       "count(*)"
       `(count *)
       "count(*)"
       '(substr "1234" 3)
       "substr(\"1234\", 3)"
       '(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
       "ST_AsText(ST_Centroid(\"MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)\"))"
       `(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
       "ST_AsText(ST_Centroid(\"MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)\"))"))

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

(deftest test-parse-expr
  (are [expr expected]
       (is (= expected (parse-expr expr)))
       1
       {:op :constant :form 1}
       1.2
       {:op :constant :form 1.2}
       "Europe"
       {:op :string :form "Europe"}
       :continents.id
       {:op :keyword :form :continents.id}
       '(max :continents.created-at)
       {:op :fn :form 'max :children [{:op :keyword :form :continents.created-at}]}
       `(max :continents.created-at)
       {:op :fn :form `max :children [{:op :keyword :form :continents.created-at}]}))
