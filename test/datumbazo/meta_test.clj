(ns datumbazo.meta-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is]]
            [datumbazo.core :as sql]
            [datumbazo.meta :as meta]
            [datumbazo.test :refer :all]))

(deftest test-best-row-identifiers
  (with-backends [db]
    (let [columns (meta/best-row-identifiers db {:table :continents})]
      (is (= [:id] (map :name columns)))
      (is (not (empty? columns)))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns)))
    (let [columns (meta/best-row-identifiers db {:table :tweets-users})]
      (is (= [:tweet-id :user-id] (map :name columns)))
      (is (not (empty? columns)))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns)))))

(deftest test-catalogs
  (with-backends [db]
    (let [catalogs (meta/catalogs db)]
      (is (not (empty? catalogs)))
      (is (every? #(keyword (:name %1)) catalogs)))))

(deftest test-columns
  (with-backends [db]
    (let [columns (meta/columns db {:table :continents})]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id :name :code :geometry :geonames-id :created-at :updated-at]
             (map :name columns))))
    (let [columns (meta/columns db {:table :countries :name :id})]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :countries (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id] (map :name columns))))
    (let [columns (meta/columns db {:schema :public :table :continents})]
      (is (= (set (map :column-name columns))
             #{"id" "name" "geometry" "updated-at" "geonames-id"
               "created-at" "code"})))
    (let [columns (meta/columns db {:schema :twitter :table :users})]
      (is (= (set (map :column-name columns))
             #{"listed-count" "lang" "url" "friends-count" "id" "name" "verified"
               "time-zone" "location" "updated-at" "profile-image-url"
               "default-profile-image" "statuses-count" "created-at"
               "followers-count" "possibly-sensitive" "screen-name" "description"
               "retweet-count"})))
    (let [columns (meta/columns db {:table :countries :name :continent-id})]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :countries (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:continent-id] (map :name columns))))))

(deftest test-columns-c3p0
  (with-backends [db {:pool :c3p0}]
    (let [columns (meta/columns db {:table :continents})]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id :name :code :geometry :geonames-id :created-at :updated-at]
             (map :name columns))))))

(deftest test-current-schema
  (with-backends [db]
    (is (= "public" (meta/current-schema db)))))

(deftest test-indexes
  (with-backends [db]
    (let [columns (meta/indexes db {:table :continents})]
      (is (not (empty? columns))))))

(deftest test-unique-columns
  (with-backends [db]
    (let [columns (meta/unique-columns db {:table :continents})]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id :name :code :geonames-id] (map :name columns))))))

(deftest test-primary-keys
  (with-backends [db]
    (let [columns (meta/primary-keys db {:table :continents})]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (= [:id] (map :name columns))))
    (let [columns (meta/primary-keys db {:table :tweets-users})]
      (is (not (empty? columns)))
      (is (every? #(= :twitter (:schema %1)) columns))
      (is (every? #(= :tweets-users (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (= [:tweet-id :user-id] (map :name columns))))))

(deftest test-schemas
  (with-backends [db]
    (let [schemas (meta/schemas db)]
      (is (not (empty? schemas)))
      (is (every? #(keyword (:name %1)) schemas))
      (is (is (set/subset? (set [:information-schema :pg-catalog :public :twitter])
                           (set (map :name schemas))))))))

(deftest test-tables
  (with-backends [db]
    @(sql/drop-table db [:test]
       (sql/if-exists true))
    (let [tables (meta/tables db)]
      (is (not (empty? tables)))
      (is (every? #(keyword? (:schema %1)) tables))
      (is (every? #(keyword? (:name %1)) tables))
      (is (every? #(= :table (:type %1)) tables))
      (is (set/subset? (set (map :name tables))
                       #{:continents
                         :countries
                         :spatial-ref-sys
                         :changes
                         :dependencies
                         :events
                         :projects
                         :tags
                         :tweets
                         :users
                         :tweets-users})))))

(deftest test-views
  (with-backends [db]
    (let [views (meta/views db)]
      (is (not (empty? views)))
      (is (every? #(keyword? (:schema %1)) views))
      (is (every? #(keyword? (:name %1)) views))
      (is (every? #(= :view (:type %1)) views)))))
