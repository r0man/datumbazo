(ns datumbazo.test
  (:require [clojure.test :refer :all]
            [clojure.spec.test.alpha :as stest]
            [datumbazo.core :as sql]
            [datumbazo.driver.core :as driver]))

(def connections
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.db"})

(def db (sql/new-db (:postgresql connections) {:backend 'jdbc.core}))

(defmacro with-test-db
  [[db-sym config & [opts]] & body]
  `(do (stest/unstrument)
       (stest/instrument)
       (sql/with-db [db# ~config (assoc ~opts :test? true)]
         (sql/with-connection [~db-sym db#]
           ~@body))))

(defmacro with-test-dbs
  [[db-sym vendors] & body]
  `(do ~@(for [[db# url#] connections
               :when (or (empty? vendors)
                         (contains? vendors db#))]
           `(testing ~(name db#)
              (with-test-db [~'db ~url#]
                ~@body)))))

(defmulti create-test-table
  (fn [db table] table))

(defmethod create-test-table :empsalary [db table]
  (sql/create-table db table
    (sql/column :depname :varchar)
    (sql/column :empno :bigint)
    (sql/column :salary :int)
    (sql/column :enroll-date :timestamp-with-time-zone)))

(defmethod create-test-table :distributors [db table]
  (sql/create-table db table
    (sql/column :did :integer :primary-key? true)
    (sql/column :dname :text :unique? true)
    (sql/column :zipcode :text)
    (sql/column :is-active :bool)))

(defmulti insert-test-table
  (fn [db table] table))

(defmethod insert-test-table :empsalary [db table]
  (sql/insert db table []
    (sql/values
     [{:depname "develop" :empno 10 :salary 5200 :enroll-date #inst "2007-08-01T00:00:00Z"}
      {:depname "sales" :empno 1 :salary 5000 :enroll-date #inst "2006-10-01T00:00:00Z"}
      {:depname "personnel" :empno 5 :salary 3500 :enroll-date #inst "2007-12-10T00:00:00Z"}
      {:depname "sales" :empno 4 :salary 4800 :enroll-date #inst "2007-08-08T00:00:00Z"}
      {:depname "personnel" :empno 2 :salary 3900 :enroll-date #inst "2006-12-23T00:00:00Z"}
      {:depname "develop" :empno 7 :salary 4200 :enroll-date #inst "2008-01-01T00:00:00Z"}
      {:depname "develop" :empno 9 :salary 4500 :enroll-date #inst "2008-01-01T00:00:00Z"}
      {:depname "sales" :empno 3 :salary 4800 :enroll-date #inst "2007-08-01T00:00:00Z"}
      {:depname "develop" :empno 8 :salary 6000 :enroll-date #inst "2006-10-01T00:00:00Z"}
      {:depname "develop" :empno 11 :salary 5200 :enroll-date #inst "2007-08-15T00:00:00Z"}])))

(defmethod insert-test-table :distributors [db table]
  (sql/insert db table []
    (sql/values
     [{:did 5 :dname "Gizmo Transglobal" :zipcode "1235"}
      {:did 6 :dname "Associated Computing, Inc" :zipcode "2356"}
      {:did 7 :dname "Redline GmbH" :zipcode "3456"}
      {:did 8 :dname "Anvil Distribution" :zipcode "4567"}
      {:did 9 :dname "Antwerp Design" :zipcode "5678"}
      {:did 10 :dname "Conrad International" :zipcode "6789"}])))

(defmacro with-drivers [[db-sym db opts] & body]
  `(doseq [driver# ['clojure.java.jdbc 'jdbc.core]]
     (if (find-ns driver#)
       (sql/with-db [~db-sym ~db (merge {:backend driver# :test? true} ~opts)]
         ~@body)
       (.println *err* (format "WARNING: Can't find %s driver, skipping tests." driver#)))))

(defmacro with-backends [[db-sym opts] & body]
  `(do (stest/unstrument)
       (stest/instrument)
       (with-drivers [db# ~(:postgresql connections) ~opts]
         (sql/with-connection [~db-sym db#]
           ~@body))))

(deftest test-with-backends
  (with-backends [db]
    (is @(sql/select db [1]))))

(deftest test-with-backends-pool
  (with-backends [db {:pool :c3p0}]
    (is (-> db :driver :datasource))))

(defmacro with-test-table [db table & body]
  `(let [db# ~db table# ~table]
     (try @(create-test-table db# table#)
          @(insert-test-table db# table#)
          ~@body
          (finally @(sql/drop-table db# [table#])))))

(defn create-companies-table [db]
  (sql/create-table db :companies
    (sql/column :id :serial :primary-key? true)))

(defn create-exchanges-table [db]
  (sql/create-table db :exchanges
    (sql/column :id :serial :primary-key? true)))

(defn create-quotes-table [db]
  (sql/create-table db :quotes
    (sql/column :id :serial :primary-key? true)
    (sql/column :exchange-id :integer :not-null? true :references :exchanges.id)
    (sql/column :company-id :integer :references :companies.id)
    (sql/column :symbol :citext :not-null? true :unique? true)
    (sql/column :created-at :timestamp-with-time-zone :not-null? true :default '(now))
    (sql/column :updated-at :timestamp-with-time-zone :not-null? true :default '(now))))
