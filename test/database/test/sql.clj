(ns database.test.sql
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.sql))

(deftest test-cascade
  (is (= [{:op :cascade :cascade true} {:cascade {:op :cascade :cascade true}}]
         ((cascade true) {}))))

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

(deftest test-continue-identity
  (is (= [{:op :continue-identity :continue-identity true} {:continue-identity {:op :continue-identity :continue-identity true}}]
         ((continue-identity true) {}))))

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

(deftest test-limit
  (is (= [{:op :limit :count 1} {:limit {:op :limit :count 1}}]
         ((limit 1) {}))))

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

(deftest test-from
  (let [[node ast] ((from :continents) {})]
    (is (= :from (:op node)))
    (let [node (first (:from node))]
      (is (= :table (:op node)))
      (is (= :continents (:name node)))))
  (let [[node ast] ((from [:continents :countries]) {})]
    (is (= :from (:op node)))
    (let [node (first (:from node))]
      (is (= :table (:op node)))
      (is (= :continents (:name node))))
    (let [node (second (:from node))]
      (is (= :table (:op node)))
      (is (= :countries (:name node)))))
  (let [[node ast] ((from (select [1 2 3])) {})]
    (is (= :from (:op node)))
    (is (= (select [1 2 3]) (first (:from node))))))

(deftest test-limit
  (is (= [{:op :limit :count 1} {:limit {:op :limit :count 1}}]
         ((limit 1) {}))))

(deftest test-offset
  (is (= [{:op :offset :start 1} {:offset {:op :offset :start 1}}]
         ((offset 1) {}))))

(deftest test-order-by
  (let [[node ast] ((order-by :created-at) {})]
    (is (= :order-by (:op node)))
    (let [node (:expr-list node)]
      (is (= :expr-list (:op node)))
      (is (= [{:op :column :schema nil :table nil :name :created-at :alias nil}] (:children node))))
    (is (= {:order-by node} ast)))
  (let [[node ast] ((order-by [:name :created-at] :desc true :nulls :first) {})]
    (is (= :order-by (:op node)))
    (is (= :desc (:direction node)))
    (is (= :first (:nulls node)))
    (let [node (:expr-list node)]
      (is (= :expr-list (:op node)))
      (is (= [{:op :column :schema nil :table nil :name :name :alias nil}
              {:op :column :schema nil :table nil :name :created-at :alias nil}] (:children node))))
    (is (= {:order-by node} ast))))

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
       :continents.created-at
       {:op :column :schema nil :table :continents :name :created-at :alias nil}
       '(greatest 1 2)
       {:op :fn :name 'greatest :args [{:op :number :form 1} {:op :number :form 2}]}
       '(max :continents.created-at)
       {:op :fn :name 'max :args [{:op :column :schema nil :table :continents :name :created-at :alias nil}]}
       `(max :continents.created-at)
       {:op :fn :name `max :args [{:op :column :schema nil :table :continents :name :created-at :alias nil}]}
       '(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
       {:op :fn :name 'ST_AsText :args [{:op :fn :name 'ST_Centroid :args [{:op :string :form "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"}]}]}))

(deftest test-restart-identity
  (is (= [{:op :restart-identity :restart-identity true} {:restart-identity {:op :restart-identity :restart-identity true}}]
         ((restart-identity true) {}))))

(deftest test-restrict
  (is (= [{:op :restrict :restrict true} {:restrict {:op :restrict :restrict true}}]
         ((restrict true) {}))))

(deftest test-select
  (are [stmt expected]
       (is (= expected (sql stmt)))
       (select 1)
       ["SELECT 1"]
       (select [1 2 3])
       ["SELECT 1, 2, 3"]
       (select [] (from :continents))
       ["SELECT * FROM continents"]
       (select * (from :continents))
       ["SELECT * FROM continents"]
       (select * (from :continents/c))
       ["SELECT * FROM continents AS c"]
       (select :created-at (from :continents))
       ["SELECT created-at FROM continents"]
       (select :created-at/c (from :continents))
       ["SELECT created-at AS c FROM continents"]
       (select [:name :created-at] (from :continents))
       ["SELECT name, created-at FROM continents"]
       (select [:name '(max :created-at)] (from :continents))
       ["SELECT name, max(created-at) FROM continents"]
       (select ['(greatest 1 2) '(lower "X")])
       ["SELECT greatest(1, 2), lower(?)" "X"]
       (select * (from :continents) (limit 1))
       ["SELECT * FROM continents LIMIT 1"]
       (select * (from :continents) (offset 1))
       ["SELECT * FROM continents OFFSET 1"]
       (select * (from :continents) (limit 1) (offset 2))
       ["SELECT * FROM continents LIMIT 1 OFFSET 2"]
       (select * (from :continents) (order-by :created-at))
       ["SELECT * FROM continents ORDER BY created-at"]
       (select * (from :continents) (order-by :created-at :asc true))
       ["SELECT * FROM continents ORDER BY created-at ASC"]
       (select * (from :continents) (order-by :created-at :desc true))
       ["SELECT * FROM continents ORDER BY created-at DESC"]
       (select * (from :continents) (order-by :created-at :nulls :first))
       ["SELECT * FROM continents ORDER BY created-at NULLS FIRST"]
       (select * (from :continents) (order-by :created-at :nulls :last))
       ["SELECT * FROM continents ORDER BY created-at NULLS LAST"]
       (select * (from :continents) (order-by [:name :created-at] :asc true))
       ["SELECT * FROM continents ORDER BY name, created-at ASC"]
       (select * (from (select [1 2 3] (as :x))))
       ["SELECT * FROM (SELECT 1, 2, 3) AS x"]
       (select * (from [(select [1] (as :x)) (select [2] (as :y))]))
       ["SELECT * FROM (SELECT 1) AS x, (SELECT 2) AS y"]))

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
