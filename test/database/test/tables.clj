(ns database.test.tables
  (:use clojure.test
        database.core
        database.columns
        database.tables
        database.fixtures
        database.registry))

(deftest test-key-columns
  (are [expected record]
    (is (= expected (map :name (key-columns (table :languages) record))))
    [] {}
    [:id] {:id 1}
    [:id :iso-639-1] {:id 1 :iso-639-1 "de"}
    [:id :iso-639-1] {:id 1 :iso-639-1 "de" :created-at "2011-12-30"}))

(deftest test-make-table
  (let [table (make-table :test [[:id :serial] [:name :text]] :url identity)]
    (is (= :test (:name table)))
    (is (= #{:id :name} (set (keys (:columns table)))))
    (is (= identity (:url table)))
    (let [columns (vals (:columns table))]
      (is (= 2 (count columns)))
      (is (every? column? columns)))))

(deftest test-table?
  (is (not (table? nil)))
  (is (not (table? "")))
  (is (table? (make-table :continents))))

(deftest test-table-name
  (are [table expected]
    (is (= expected (table-name table)))
    "photo-thumbnails" "photo-thumbnails"
    'photo-thumbnails "photo-thumbnails"
    :photo-thumbnails "photo-thumbnails"
    (find-table :photo-thumbnails) "photo-thumbnails"))

(deftest test-table-identifier
  (are [table expected]
    (is (= expected (table-identifier table)))
    "photo-thumbnails" "photo_thumbnails"
    'photo-thumbnails "photo_thumbnails"
    :photo-thumbnails "photo_thumbnails"
    (find-table :photo-thumbnails) "photo_thumbnails"))

(deftest test-default-columns
  (let [columns (default-columns (table :languages))]
    (is (every? column? columns))
    (is (= [:updated-at :created-at :iso-639-2 :iso-639-1 :family :name :id]
           (map :name columns)))))

(deftest test-select-columns
  [empty? (select-columns (table :languages) [])]
  (let [columns (select-columns (table :languages) [:id])]
    (is (every? column? columns))
    (is (= [:id] (map :name columns)))))

(deftest test-primary-key-columns
  (is (= [:id] (map :name (primary-key-columns (find-table :languages))))))

(deftest test-unique-columns
  (is (= [:iso-639-2 :iso-639-1 :name]
         (map :name (unique-columns (find-table :languages))))))
