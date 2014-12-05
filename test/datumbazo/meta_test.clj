(ns datumbazo.meta-test
  (:require [clojure.set :refer [subset?]]
            [clojure.test :refer :all]
            [datumbazo.meta :refer :all]
            [datumbazo.test :refer :all])
  (:import java.sql.DatabaseMetaData))

(deftest test-best-row-identifiers
  (with-test-db [db]
    (let [columns (best-row-identifiers db :table :continents)]
      (is (= [:id] (map :name columns)))
      (is (not (empty? columns)))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns)))
    (let [columns (best-row-identifiers db :table :tweets-users)]
      (is (= [:tweet-id :user-id] (map :name columns)))
      (is (not (empty? columns)))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns)))))

(deftest test-catalogs
  (with-test-db [db]
    (let [catalogs (catalogs db)]
      (is (not (empty? catalogs)))
      (is (every? #(keyword (:name %1)) catalogs)))))

(deftest test-columns
  (with-test-db [db]
    (let [columns (columns db :table :continents)]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id :name :code :geometry :freebase-guid :geonames-id :created-at :updated-at]
             (map :name columns))))
    (let [columns (columns db :table :countries :name :id)]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :countries (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id] (map :name columns))))
    (let [columns (columns db :schema :public :table :users)]
      (is (= (set (map :column-name columns))
             #{"id" "name" "created_at" "updated_at"})))
    (let [columns (columns db :schema :twitter :table :users)]
      (is (= (set (map :column-name columns))
             #{"listed_count" "lang" "url" "friends_count" "id" "name" "verified"
               "time_zone" "location" "updated_at" "profile_image_url"
               "default_profile_image" "statuses_count" "created_at"
               "followers_count" "possibly_sensitive" "screen_name" "description"
               "retweet_count"})))
    ;; TODO: Fixme
    ;; (let [columns (columns db :table :countries :name :continent-id)]
    ;;   (is (not (empty? columns)))
    ;;   (is (every? #(= :public (:schema %1)) columns))
    ;;   (is (every? #(= :countries (:table %1)) columns))
    ;;   (is (every? #(keyword? (:name %1)) columns))
    ;;   (is (every? #(keyword? (:type %1)) columns))
    ;;   (is (= [:continent-id] (map :name columns))))
    ))

(deftest test-columns-c3p0
  (with-test-db [db "c3p0:postgresql://tiger:scotch@localhost/datumbazo"]
    (let [columns (columns db :table :continents)]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id :name :code :geometry :freebase-guid :geonames-id :created-at :updated-at]
             (map :name columns))))))

(deftest test-indexes
  (with-test-db [db]
    (let [columns (indexes db :table :continents)]
      (is (not (empty? columns))))))

(deftest test-unique-columns
  (with-test-db [db]
    (let [columns (unique-columns db :table :continents)]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (every? #(keyword? (:type %1)) columns))
      (is (= [:id :name :code :freebase-guid :geonames-id] (map :name columns))))))

(deftest test-primary-keys
  (with-test-db [db]
    (let [columns (primary-keys db :table :continents)]
      (is (not (empty? columns)))
      (is (every? #(= :public (:schema %1)) columns))
      (is (every? #(= :continents (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (= [:id] (map :name columns))))
    (let [columns (primary-keys db :table :tweets-users)]
      (is (not (empty? columns)))
      (is (every? #(= :twitter (:schema %1)) columns))
      (is (every? #(= :tweets-users (:table %1)) columns))
      (is (every? #(keyword? (:name %1)) columns))
      (is (= [:tweet-id :user-id] (map :name columns))))))

(deftest test-schemas
  (with-test-db [db]
    (let [schemas (schemas db)]
      (is (not (empty? schemas)))
      (is (every? #(keyword (:name %1)) schemas))
      (is (is (subset? (set [:information-schema :pg-catalog :public :twitter])
                       (set (map :name schemas))))))))

(deftest test-tables
  (with-test-db [db]
    (let [tables (tables db)]
      (is (not (empty? tables)))
      (is (every? #(keyword? (:schema %1)) tables))
      (is (every? #(keyword? (:name %1)) tables))
      (is (every? #(= :table (:type %1)) tables))
      (is (subset? (set (map :name tables))
                   (set [:continents :countries :spatial-ref-sys :changes :dependencies :events :projects :tags :tweets :users :tweets-users]))))))

(deftest test-views
  (with-test-db [db]
    (let [views (views db)]
      (is (not (empty? views)))
      (is (every? #(keyword? (:schema %1)) views))
      (is (every? #(keyword? (:name %1)) views))
      (is (every? #(= :view (:type %1)) views)))))
