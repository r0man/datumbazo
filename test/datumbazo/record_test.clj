(ns datumbazo.record-test
  (:refer-clojure :exclude [update])
  (:require [clojure.test :refer :all]
            [datumbazo.record :refer :all]
            [datumbazo.test :refer :all]
            [sqlingvo.core :as sql :refer [column]]
            [datumbazo.util :as util])
  (:import datumbazo.continents.Continent))

(def continents-table
  (util/table-by-class Continent))

(deftest test-define-instance?
  (is (= (#'datumbazo.record/define-instance? continents-table)
         '(clojure.core/defn continent?
            "Return true if `x` is a continent, otherwise false."
            [x]
            (clojure.core/instance? Continent x)))))

(deftest test-define-make-instance
  (is (= (#'datumbazo.record/define-make-instance continents-table)
         '(clojure.core/defmethod datumbazo.util/make-instance Continent
            [class attrs & [db]]
            (clojure.core/->
             (new Continent db attrs (clojure.core/meta attrs))
             (datumbazo.callbacks/after-initialize))))))

(deftest test-define-find-all
  (is (= (#'datumbazo.record/define-find-all continents-table)
         '(clojure.core/defn all
            "Find all rows in `db`."
            [db & [opts]]
            (datumbazo.record/find-all db Continent opts)))))

(deftest test-on-conflict-clause
  (is (= (#'datumbazo.record/on-conflict-clause Continent)
         [:name])))

(deftest test-do-update-clause
  (is (= (#'datumbazo.record/do-update-clause Continent)
         {:created-at :EXCLUDED.created-at,
          :code :EXCLUDED.code,
          :updated-at :EXCLUDED.updated-at})))

(deftest test-primary-key-columns
  (is (= (#'datumbazo.record/primary-key-columns Continent)
         #{{:schema nil,
            :table :continents,
            :primary-key? true,
            :default nil,
            :name :id,
            :type :serial,
            :op :column}})))

(deftest test-unique-key-columns
  (is (= (#'datumbazo.record/unique-key-columns Continent)
         #{{:schema nil
            :table :continents
            :default nil
            :name :name
            :type :varchar
            :op :column
            :unique? true}
           {:schema nil
            :table :continents
            :default nil
            :name :code
            :type :varchar
            :op :column
            :unique? true}})))

(deftest test-select-class
  (is (= (sql/sql (select-class db Continent))
         [(str "SELECT \"continents\".\"id\", \"continents\".\"created-at\", "
               "\"continents\".\"name\", \"continents\".\"code\", "
               "\"continents\".\"updated-at\" FROM \"continents\"")])))
