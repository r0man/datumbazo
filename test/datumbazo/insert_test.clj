(ns datumbazo.insert-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]
            [geo.postgis :as geo]))

(deftest test-deref-insert
  (with-backends [db]
    (let [table :test-deref-insert]
      @(sql/create-table db table
         (sql/column :a :integer)
         (sql/column :b :integer))
      (is (= @(sql/insert db table [:a :b]
                (sql/values [{:a 1 :b 2}]))
             [{:count 1}]))
      @(sql/drop-table db [table]))))

(deftest test-with-insert
  (with-backends [db]
    @(sql/create-table db :a
       (sql/column :id :integer))
    @(sql/insert db :a [:id]
       (sql/values [{:id 1}]))
    (is (= @(sql/insert db :a [:id]
              (sql/with db [:x (sql/select db [:id] (sql/from :a))]
                (sql/select db [:id]
                  (sql/from :x))))
           [{:count 1}]))
    (is (= @(sql/select db [:*]
              (sql/from :a))
           [{:id 1} {:id 1}]))))

(deftest test-insert-fixed-columns-mixed-values
  (with-backends [db]
    @(sql/drop-table db [:test] (sql/if-exists true))
    @(sql/create-table db :test
       (sql/column :a :integer)
       (sql/column :b :integer))
    (is (= @(sql/insert db :test [:a :b]
              (sql/values [{:a 1 :b 2} {:b 3} {:c 3}])
              (sql/returning :*))
           [{:a 1 :b 2}
            {:a nil :b 3}
            {:a nil :b nil}]))))

(deftest test-insert-fixed-columns-mixed-values-2
  (with-backends [db]
    @(create-companies-table db)
    @(sql/insert db :companies [:id]
       (sql/values [{:id 5}]))
    @(create-exchanges-table db)
    @(sql/insert db :exchanges [:id]
       (sql/values [{:id 2}]))
    @(create-quotes-table db)
    (is (= @(sql/insert db :quotes [:id :exchange-id :company-id
                                    :symbol :created-at :updated-at]
              (sql/values [{:updated-at #inst "2012-11-02T18:22:59.688-00:00"
                            :created-at #inst "2012-11-02T18:22:59.688-00:00"
                            :symbol "MSFT"
                            :exchange-id 2
                            :company-id 5
                            :id 5}
                           {:updated-at #inst "2012-11-02T18:22:59.688-00:00"
                            :created-at #inst "2012-11-02T18:22:59.688-00:00"
                            :symbol "SPY"
                            :exchange-id 2
                            :id 6}])
              (sql/returning :*))
           [{:updated-at #inst "2012-11-02T18:22:59.688-00:00",
             :created-at #inst "2012-11-02T18:22:59.688-00:00",
             :symbol "MSFT",
             :company-id 5,
             :exchange-id 2,
             :id 5}
            {:updated-at #inst "2012-11-02T18:22:59.688-00:00",
             :created-at #inst "2012-11-02T18:22:59.688-00:00",
             :symbol "SPY",
             :company-id nil,
             :exchange-id 2,
             :id 6}]))))

(deftest test-insert-array
  (with-backends [db]
    @(sql/drop-table db [:test] (sql/if-exists true))
    @(sql/create-table db :test
       (sql/column :x :text :array? true))
    (is (= @(sql/insert db :test [:x]
              (sql/values [{:x [1 2]}
                           {:x [3 4]}])
              (sql/returning :*))
           [{:x ["1" "2"]}
            {:x ["3" "4"]}]))))

(deftest test-insert-on-conflict-do-update
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db :distributors [:did :dname]
                (sql/values [{:did 5 :dname "Gizmo Transglobal"}
                             {:did 6 :dname "Associated Computing, Inc"}])
                (sql/on-conflict [:did]
                  (sql/do-update {:dname :EXCLUDED.dname})))
             [{:count 2}]))
      (is (= @(sql/select db [:*]
                (sql/from :distributors)
                (sql/where '(in :did (5 6)))
                (sql/order-by :did))
             [{:did 5
               :dname "Gizmo Transglobal"
               :zipcode "1235"
               :is-active nil}
              {:did 6
               :dname "Associated Computing, Inc"
               :zipcode "2356"
               :is-active nil}])))))

(deftest test-insert-on-conflict-do-nothing
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db :distributors [:did :dname]
                (sql/values [{:did 7 :dname "Redline GmbH"}])
                (sql/on-conflict [:did]
                  (sql/do-nothing)))
             [{:count 0}])))))

(deftest test-insert-on-conflict-do-nothing-returning
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db :distributors [:did :dname]
                (sql/values [{:did 11 :dname "Upsert GmbH & Co. KG"}])
                (sql/on-conflict [:did]
                  (sql/do-nothing))
                (sql/returning :*))
             [{:did 11,
               :dname "Upsert GmbH & Co. KG",
               :zipcode nil,
               :is-active nil}]))
      (is (empty? @(sql/insert db :distributors [:did :dname]
                     (sql/values [{:did 11 :dname "Upsert GmbH & Co. KG"}])
                     (sql/on-conflict [:did]
                       (sql/do-nothing))
                     (sql/returning :*)))))))

(deftest test-insert-on-conflict-do-update-where
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db (sql/as :distributors :d) [:did :dname]
                (sql/values [{:did 8 :dname "Anvil Distribution"}])
                (sql/on-conflict [:did]
                  (sql/do-update {:dname '(:|| :EXCLUDED.dname " (formerly " :d.dname ")")})
                  (sql/where '(:<> :d.zipcode "21201"))))
             [{:count 1}]))
      (is (= @(sql/select db [:*]
                (sql/from :distributors)
                (sql/where `(= :did 8)))
             [{:did 8
               :dname "Anvil Distribution (formerly Anvil Distribution)"
               :zipcode "4567"
               :is-active nil}])))))

(deftest test-insert-on-conflict-do-update-returning
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db (sql/as :distributors :d) [:did :dname]
                (sql/values [{:did 8 :dname "Anvil Distribution"}])
                (sql/on-conflict [:did]
                  (sql/do-update {:dname '(:|| :EXCLUDED.dname " (formerly " :d.dname ")")})
                  (sql/where '(:<> :d.zipcode "21201")))
                (sql/returning :*))
             [{:did 8,
               :dname "Anvil Distribution (formerly Anvil Distribution)",
               :zipcode "4567",
               :is-active nil}])))))

(deftest test-insert-on-conflict-where-do-nothing
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db :distributors [:did :dname]
                (sql/values [{:did 10 :dname "Conrad International"}])
                (sql/on-conflict [:did]
                  (sql/where '(= :is-active true))
                  (sql/do-nothing)))
             [{:count 0}]))
      (is (= @(sql/select db [:*]
                (sql/from :distributors)
                (sql/where `(= :did 10)))
             [{:did 10
               :dname "Conrad International"
               :zipcode "6789"
               :is-active nil}])))))

(deftest test-insert-on-conflict-on-constraint-do-nothing
  (with-backends [db]
    (with-test-table db :distributors
      (is (= @(sql/insert db :distributors [:did :dname]
                (sql/values [{:did 9 :dname "Antwerp Design"}])
                (sql/on-conflict-on-constraint :distributors_pkey
                  (sql/do-nothing)))
             [{:count 0}]))
      (is (= @(sql/select db [:*]
                (sql/from :distributors)
                (sql/where `(= :did 9)))
             [{:did 9
               :dname "Antwerp Design"
               :zipcode "5678"
               :is-active nil}])))))

(deftest test-insert-bigint
  (with-backends [db]
    @(sql/create-table db :test-insert-bigint
       (sql/column :id :bigint))
    (is (= @(sql/insert db :test-insert-bigint [:id]
              (sql/values [{:id (bigint 1)}])
              (sql/returning :*))
           [{:id 1}]))))

(deftest test-insert-geometry-column
  (with-backends [db]
    (let [table :test-create-geometry-column]
      @(sql/create-table db table
         (sql/column :location :geometry))
      (is (= @(sql/insert db table []
                (sql/values [{:location (geo/point 4326 1 2)}])
                (sql/returning :*))
             [{:location (geo/point 4326 1 2)}])))))

(deftest test-insert-geography-column
  (with-backends [db]
    (let [table :test-create-geography-column]
      @(sql/create-table db table
         (sql/column :location :geography))
      (is (= @(sql/insert db table []
                (sql/values [{:location (geo/point 4326 1 2)}])
                (sql/returning :*))
             [{:location (geo/point 4326 1 2)}])))))
