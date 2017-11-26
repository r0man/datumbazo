(ns datumbazo.create-table-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]))

(defn create-measurement
  "Create the measurement table."
  [db]
  @(sql/create-table db :measurement
     (sql/column :city-id :int :not-null? true)
     (sql/column :logdate :date :not-null? true)
     (sql/column :peaktmp :int)
     (sql/column :unitsales :int)))

(deftest test-create-measurement
  (with-backends [db db]
    (is (= (create-measurement db)
           [{:count 0}]))))

(deftest test-create-table
  (with-test-dbs [db]
    (when (= (:scheme db) :mysql)
      (let [table :test-create-table]
        (try (is (= @(sql/create-table db table
                       (sql/column :id :integer)
                       (sql/column :nick :varchar :size 32)
                       (sql/primary-key :nick))
                    [{:count 0}]))
             (finally
               ;; Cleanup for MySQL (non-transactional DDL)
               @(sql/drop-table db [table]
                  (sql/if-exists true))))))))

(deftest test-create-table-inherits-check
  (with-backends [db db]
    (create-measurement db)
    (is (= @(sql/create-table db :measurement-y2006m02
              (sql/check '(and (>= :logdate "2006-02-01") (< :logdate "2006-03-01")))
              (sql/inherits :measurement))
           [{:count 0}]))))

(deftest test-create-table-inherits-check-multiple
  (with-backends [db db]
    (create-measurement db)
    (is (= @(sql/create-table db :measurement-y2006m02
              (sql/check '(>= :logdate "2006-02-01"))
              (sql/check '(< :logdate "2006-03-01"))
              (sql/inherits :measurement))
           [{:count 0}]))))

(deftest test-create-table-compound-primary-key
  (with-backends [db]
    @(sql/create-table db :users
       (sql/column :id :serial :primary-key? true))
    @(sql/create-table db :spots
       (sql/column :id :serial :primary-key? true))
    (is (= @(sql/create-table db :ratings
              (sql/column :id :serial)
              (sql/column :user-id :integer :not-null? true :references :users.id)
              (sql/column :spot-id :integer :not-null? true :references :spots.id)
              (sql/column :rating :integer :not-null? true)
              (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
              (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))
              (sql/primary-key :user-id :spot-id :created-at))
           [{:count 0}]))))

(deftest test-create-table-dertef
  (with-backends [db]
    (let [table :test-deref-create-table]
      (is (= @(sql/create-table db table
                (sql/column :a :integer)
                (sql/column :b :integer))
             [{:count 0}]))
      @(sql/drop-table db [table]))))

(deftest test-create-table-array-column
  (with-backends [db]
    (is @(sql/drop-table db [:test] (sql/if-exists true)))
    (is @(sql/create-table db :test
           (sql/column :x :text :array? true)))))
