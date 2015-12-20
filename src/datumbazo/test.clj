(ns datumbazo.test
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [datumbazo.driver.core :refer [close-db open-db]]
            [datumbazo.core :refer :all]))

(def connections
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.db"})

(def db (new-db (:postgresql connections)))

(defmacro with-test-db
  [[db-sym connection] & body]
  `(let [db# (new-db (or ~connection ~(:postgresql connections)))]
     (with-db [~db-sym (assoc db# :test true)]
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

(defmacro with-backends [[db-sym opts] & body]
  `(doseq [backend# ['clojure.java.jdbc 'jdbc.core]]
     (if (find-ns backend#)
       (let [db# (open-db (assoc ~db :backend backend#)), ~db-sym db#]
         (try (testing (str "Testing backend " (str backend#))
                (if (:rollback? ~opts)
                  (with-transaction [~db-sym ]~@body)
                  (do ~@body)))
              (finally (close-db db#))))
       (.println *err* (format "WARNING: Can't find %s backend, skipping tests." backend#)))))

(defmacro with-test-table [db table & body]
  `(let [db# ~db table# ~table]
     (try @(create-test-table db# table#)
          @(insert-test-table db# table#)
          ~@body
          (finally @(drop-table db# [table#])))))
