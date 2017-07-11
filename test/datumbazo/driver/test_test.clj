(ns datumbazo.driver.test-test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.meta :as meta]
            [datumbazo.test :refer :all])
  (:import java.sql.Connection))

(deftest test-begin
  (with-drivers [db db]
    (is (thrown? AssertionError (sql/begin db)))
    (sql/with-connection [db db]
      (let [db (sql/begin db)]
        (is (not (.getAutoCommit (sql/connection db))))
        @(sql/select db [1])))))

(deftest test-connect
  (with-drivers [db db]
    (let [db (sql/connect db)]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection
  (with-drivers [db db]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-bonecp
  (with-drivers [db db]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-c3p0
  (with-drivers [db db]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db))))))

(deftest test-connection-with-hikaricp
  (with-drivers [db db]
    (is (nil? (sql/connection db)))
    (sql/with-connection [db db]
      (is (instance? Connection (sql/connection db)))
      @(sql/select db [1]))))

(deftest test-commit
  (with-drivers [db db]
    (is (thrown? AssertionError (sql/commit db)))
    (sql/with-connection [db db]
      (let [db (sql/begin db)]
        @(sql/select db [1])
        (let [db (sql/commit db)]
          (is db))))))

(deftest test-disconnect
  (with-drivers [db db]
    (is (thrown? AssertionError (sql/disconnect db)))
    (let [db (sql/disconnect (sql/connect db))]
      (is (nil? (sql/connection db))))))

(deftest test-fetch
  (with-drivers [db db]
    (is (= @(sql/select db [1])
           [{:?column? 1}]))))

(deftest test-execute
  (with-drivers [db db]
    (is (= @(sql/create-table db :test-execute
              (sql/column :id :serial))
           [{:count 0}]))))

(deftest test-rollback
  (with-drivers [db db]
    (is (thrown? AssertionError (sql/rollback db)))
    (sql/with-connection [db db]
      (let [db (sql/begin db)]
        @(sql/create-table db :test-rollback
           (sql/column :id :integer))
        (is (= (sql/rollback db) db))
        (sql/commit db)))))

(deftest test-with-transaction
  (with-drivers [db db]
    (sql/with-transaction [db db]
      (is (instance? Connection (sql/connection db))))
    (sql/with-connection [db db]
      (sql/with-transaction [db db]
        @(sql/create-table db :with-transaction
           (sql/column :id :integer))
        (sql/rollback db)))
    (sql/with-connection [db db]
      (is (empty? (meta/tables db {:name :with-transaction}))))))

(deftest test-race-clojure-java-jdbc
  (sql/with-db [db (:postgresql connections) {:backend 'clojure.java.jdbc :test? true}]
    (let [counter (atom 0)]
      @(sql/drop-table db [:test]
         (sql/if-exists true))
      @(sql/create-table db :test
         (sql/column :id :uuid))
      (doall (pmap (fn [uuid]
                     (sql/with-transaction [db db]
                       @(sql/insert db :test [:id]
                          (sql/values [[uuid]])
                          (sql/returning :*))
                       (swap! counter inc)))
                   (take 10 (repeatedly #(java.util.UUID/randomUUID)))))
      (is (= (->> @(sql/select db ['(count :*)]
                     (sql/from :test))
                  first :count)
             @counter))
      @(sql/drop-table db [:test]
         (sql/if-exists true)))))

;; (deftest test-race-fail-jdbc-core
;;   (sql/with-db [db (:postgresql connections) {:backend 'jdbc.core}]
;;     (let [counter (atom 0)]
;;       @(sql/drop-table db [:test]
;;          (sql/if-exists true))
;;       @(sql/create-table db :test
;;          (sql/column :id :uuid))
;;       (doall (pmap (fn [uuid]
;;                      (sql/with-transaction [db db]
;;                        @(sql/insert db :test [:id]
;;                           (sql/values [[uuid]])
;;                           (sql/returning :*))
;;                        (swap! counter inc)))
;;                    (take 5 (repeatedly #(java.util.UUID/randomUUID)))))
;;       (is (= (->> @(sql/select db ['(count :*)]
;;                      (sql/from :test))
;;                   first :count)
;;              @counter)))))

(comment

  (sql/with-db [db (:postgresql connections) {:backend 'jdbc.core :test? true}]
    ;; (sql/connection db)
    (sql/with-connection [db db]
      (prn (sql/connection db))
      (sql/with-transaction [db db]
        (prn (sql/connection db)))
      (sql/with-transaction [db db]
        (prn (sql/connection db)))))

  (sql/with-db [db (:postgresql connections) {:backend 'jdbc.core :test? true}]
    (sql/with-transaction [db db]
      @(sql/select db [1])))

  (sql/with-db [db (:postgresql connections) {:backend 'jdbc.core :test? true}]
    @(sql/select db [1]))

  )
