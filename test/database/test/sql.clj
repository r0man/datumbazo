(ns database.test.sql
  (:use clojure.test
        database.sql))

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
