(ns datumbazo.test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer :all]
            [datumbazo.driver.core :as driver]
            [schema.core :as s]))

(s/set-fn-validation! true)

(def connections
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.db"})

(def db (new-db (:postgresql connections) {:backend 'jdbc.core}))

(defmacro with-test-db
  [[db-sym config & [opts]] & body]
  `(with-db [db# ~config (assoc ~opts :test? true)]
     (with-connection [~db-sym db#]
       ~@body)))

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
  (create-table db table
    (column :depname :varchar)
    (column :empno :bigint)
    (column :salary :int)
    (column :enroll-date :timestamp-with-time-zone)))

(defmethod create-test-table :distributors [db table]
  (create-table db table
    (column :did :integer :primary-key? true)
    (column :dname :text)
    (column :zipcode :text)
    (column :is-active :bool)))

(defmulti insert-test-table
  (fn [db table] table))

(defmethod insert-test-table :empsalary [db table]
  (insert db table []
    (values
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
  (insert db table []
    (values
     [{:did 5 :dname "Gizmo Transglobal" :zipcode "1235"}
      {:did 6 :dname "Associated Computing, Inc" :zipcode "2356"}
      {:did 7 :dname "Redline GmbH" :zipcode "3456"}
      {:did 8 :dname "Anvil Distribution" :zipcode "4567"}
      {:did 9 :dname "Antwerp Design" :zipcode "5678"}
      {:did 10 :dname "Conrad International" :zipcode "6789"}])))

(defmacro with-drivers [[db-sym db opts] & body]
  `(doseq [driver# ['clojure.java.jdbc 'jdbc.core]]
     (if (find-ns driver#)
       (with-db [~db-sym ~db (merge {:backend driver# :test? true} ~opts)]
         ~@body)
       (.println *err* (format "WARNING: Can't find %s driver, skipping tests." driver#)))))

(defmacro with-backends [[db-sym opts] & body]
  `(with-drivers [db# ~(:postgresql connections) ~opts]
     (with-connection [~db-sym db#]
       ~@body)))

(deftest test-with-backends
  (with-backends [db]
    (is @(select db [1]))))

(deftest test-with-backends-pool
  (with-backends [db {:pool :c3p0}]
    (is (:datasource db))))

(defmacro with-test-table [db table & body]
  `(let [db# ~db table# ~table]
     (try @(create-test-table db# table#)
          @(insert-test-table db# table#)
          ~@body
          (finally @(drop-table db# [table#])))))
