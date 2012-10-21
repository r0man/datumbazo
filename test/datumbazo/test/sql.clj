(ns datumbazo.test.sql
  (:refer-clojure :exclude [group-by replace])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        datumbazo.sql))

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

(deftest test-group-by
  (let [node (:group-by (group-by {} :name :created-at))]
    (is (= :group-by (:op node)))
    (let [expressions (:expressions node)]
      (let [node (first (:children expressions))]
        (is (= :column (:op node)))
        (is (= :name (:name node))))
      (let [node (second (:children expressions))]
        (is (= :column (:op node)))
        (is (= :created-at (:name node)))))))

(deftest test-limit
  (is (= {:limit {:op :limit :count 1}} (limit {} 1))))

(deftest test-drop-table
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (drop-table :continents)
       ["DROP TABLE continents"]
       (drop-table [:continents :countries])
       ["DROP TABLE continents, countries"]
       (drop-table :continents :if-exists true :restrict true)
       ["DROP TABLE IF EXISTS continents RESTRICT"]))

(deftest test-from
  (let [node (:from (from {} :continents))]
    (is (= :from (:op node)))
    (let [node (first (:from node))]
      (is (= :table (:op node)))
      (is (= :continents (:name node)))))
  (let [node (:from (from {} :continents :countries))]
    (is (= :from (:op node)))
    (let [node (first (:from node))]
      (is (= :table (:op node)))
      (is (= :continents (:name node))))
    (let [node (second (:from node))]
      (is (= :table (:op node)))
      (is (= :countries (:name node)))))
  ;; (let [node (:from (from (select 1 2 3)))]
  ;;   (println node)
  ;;   (is (= :from (:op node)))
  ;;   (is (= (select [1 2 3]) (first (:from node)))))

  )

(deftest test-offset
  (is (= {:offset {:op :offset :start 1}} (offset {} 1))))

(deftest test-order-by
  (let [node (:order-by (order-by {} :created-at))]
    (is (= :order-by (:op node)))
    (let [node (:expressions node)]
      (is (= :expressions (:op node)))
      (is (= [{:op :column :schema nil :table nil :name :created-at :alias nil}] (:children node)))))
  (let [node (:order-by (order-by {} [:name :created-at] :direction :desc :nulls :first))]
    (is (= :order-by (:op node)))
    (is (= :desc (:direction node)))
    (is (= :first (:nulls node)))
    (let [node (:expressions node)]
      (is (= :expressions (:op node)))
      (is (= [{:op :column :schema nil :table nil :name :name :alias nil}
              {:op :column :schema nil :table nil :name :created-at :alias nil}] (:children node))))))

(deftest test-select
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (select 1)
       ["SELECT 1"]
       (select (as 1 :n))
       ["SELECT 1 AS n"]
       (select (as "s" :s))
       ["SELECT ? AS s" "s"]
       (select 1 2 3)
       ["SELECT 1, 2, 3"]
       (select (as 1 :a) (as 2 :b) (as 3 :c))
       ["SELECT 1 AS a, 2 AS b, 3 AS c"]
       (-> (select) (from :continents))
       ["SELECT * FROM continents"]
       (-> (select *) (from :continents))
       ["SELECT * FROM continents"]
       (-> (select *) (from :continents/c))
       ["SELECT * FROM continents AS c"]
       (-> (select :created-at) (from :continents))
       ["SELECT created-at FROM continents"]
       (-> (select :created-at/c) (from :continents))
       ["SELECT created-at AS c FROM continents"]
       (-> (select :name :created-at) (from :continents))
       ["SELECT name, created-at FROM continents"]
       (-> (select :name '(max :created-at)) (from :continents))
       ["SELECT name, max(created-at) FROM continents"]
       (select '(greatest 1 2) '(lower "X"))
       ["SELECT greatest(1, 2), lower(?)" "X"]
       (-> (select (as '(max :created-at) :m)) (from :continents))
       ["SELECT max(created-at) AS m FROM continents"]
       (-> (select *) (from :continents) (limit 1))
       ["SELECT * FROM continents LIMIT 1"]
       (-> (-> (select *)) (from :continents) (offset 1))
       ["SELECT * FROM continents OFFSET 1"]
       (-> (-> (select *)) (from :continents) (limit 1) (offset 2))
       ["SELECT * FROM continents LIMIT 1 OFFSET 2"]
       (-> (select *) (from :continents) (order-by :created-at))
       ["SELECT * FROM continents ORDER BY created-at"]
       (-> (select *) (from :continents) (order-by :created-at :direction :asc))
       ["SELECT * FROM continents ORDER BY created-at ASC"]
       (-> (select *) (from :continents) (order-by :created-at :direction :desc))
       ["SELECT * FROM continents ORDER BY created-at DESC"]
       (-> (select *) (from :continents) (order-by :created-at :nulls :first))
       ["SELECT * FROM continents ORDER BY created-at NULLS FIRST"]
       (-> (select *) (from :continents) (order-by :created-at :nulls :last))
       ["SELECT * FROM continents ORDER BY created-at NULLS LAST"]
       (-> (select *) (from :continents) (order-by [:name :created-at] :direction :asc))
       ["SELECT * FROM continents ORDER BY name, created-at ASC"]
       (-> (select *)
           (from (as (select 1 2 3) :x)))
       ["SELECT * FROM (SELECT 1, 2, 3) AS x"]
       (-> (select *)
           (from (as (select 1) :x) (as (select 2) :y)))
       ["SELECT * FROM (SELECT 1) AS x, (SELECT 2) AS y"]
       (-> (select *) (from :continents) (group-by :created-at))
       ["SELECT * FROM continents GROUP BY created-at"]
       (-> (select *) (from :continents) (group-by :name :created-at))
       ["SELECT * FROM continents GROUP BY name, created-at"]))

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
       (truncate-table :continents :cascade true :continue-identity true :restart-identity true :restrict true)
       ["TRUNCATE TABLE continents RESTART IDENTITY CONTINUE IDENTITY CASCADE RESTRICT"]))
