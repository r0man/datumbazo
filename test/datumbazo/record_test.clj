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
             (new Continent
                  db
                  (datumbazo.util/row->record Continent attrs)
                  (clojure.core/meta attrs))
             (datumbazo.callbacks/after-initialize))))))

(deftest test-define-find-all
  (is (= (#'datumbazo.record/define-find-all continents-table)
         '(clojure.core/defn all
            "Find all rows in `db`."
            [db & [opts]]
            {:pre [(sqlingvo.core/db? db)]}
            (datumbazo.record/find-all db Continent opts)))))

(deftest test-on-conflict-clause
  (is (= (#'datumbazo.record/on-conflict-clause Continent)
         [:id])))

(deftest test-do-update-clause
  (is (= (#'datumbazo.record/do-update-clause Continent)
         {:created-at :EXCLUDED.created-at,
          :code :EXCLUDED.code,
          :geometry :EXCLUDED.geometry
          :geonames-id :EXCLUDED.geonames-id
          :updated-at :EXCLUDED.updated-at})))

(deftest test-primary-key-columns
  (is (= (#'datumbazo.record/primary-key-columns Continent)
         #{{:schema nil,
            :children [:name],
            :table :continents,
            :primary-key? true,
            :default nil,
            :name :id,
            :val :id,
            :type :serial,
            :op :column,
            :form :id}})))

(deftest test-unique-key-columns
  (is (= (#'datumbazo.record/unique-key-columns Continent)
         #{{:schema nil,
            :children [:name],
            :not-null? true,
            :table :continents,
            :default nil,
            :name :name,
            :val :name,
            :type :text,
            :op :column,
            :unique? true,
            :form :name}
           {:schema nil,
            :children [:name],
            :not-null? true,
            :table :continents,
            :default nil,
            :name :code,
            :val :code,
            :type :varchar,
            :op :column,
            :size 2,
            :unique? true,
            :form :code}
           {:schema nil,
            :children [:name],
            :table :continents,
            :default nil,
            :name :geonames-id,
            :val :geonames-id,
            :type :integer,
            :op :column,
            :unique? true,
            :form :geonames-id}})))

(deftest test-select-class
  (is (= (sql/sql (select-class db Continent))
         [(str "SELECT \"continents\".\"created-at\", "
               "\"continents\".\"name\", "
               "\"continents\".\"code\", "
               "\"continents\".\"updated-at\", "
               "CAST(\"continents\".\"geometry\" AS GEOMETRY) AS \"geometry\", "
               "\"continents\".\"id\", "
               "\"continents\".\"geonames-id\" "
               "FROM \"continents\"")])))
