(ns database.test.meta
  (:require [clj-time.core :refer [now]]
            [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.meta
        database.protocol
        database.test
        database.util))

(def columns
  {:created-at (make-column :continents/created-at :timestamp-with-time-zone :default "now()" :not-null? true)
   :id (make-column :languages/id :serial :primary-key? true)
   :iso-639-1 (make-column :languages/iso-639-1 :varchar :length 2 :unique? true :not-null? true)
   :location (make-column :continents/location [:point-2d])})

(deftest test-as-identifier
  (testing "with schema"
    (is (= "public" (as-identifier (make-schema :public)))))
  (with-quoted-identifiers \"
    (testing "with schema"
      (is (= "\"public\"" (as-identifier (make-schema :public)))))))

;; SCHEMAS

(database-test test-load-schemas
  (is (load-schemas))
  (is (pos? (count @*schemas*))))

(deftest test-lookup-schema
  (is (nil? (lookup-schema :unknown-schema)))
  (let [schema (register-schema (make-schema :oauth))]
    (is (= schema (lookup-schema schema)))
    (is (= schema (lookup-schema (:name schema))))))

(deftest test-make-schema
  (is (thrown? AssertionError (make-schema nil)))
  (is (= (make-schema :public) (make-schema "public")))
  (let [schema (make-schema :public)]
    (is (= :public (:name schema)))))

(deftest test-register-schema
  (with-frozen-time (now)
    (let [schema (make-schema :oauth)]
      (is (= (assoc schema :registered-at (now))
             (register-schema schema)))
      (is (= (assoc schema :registered-at (now))
             (get @*schemas* (:name schema)))))))

(deftest test-schema?
  (is (not (schema? nil)))
  (is (not (schema? "")))
  (is (schema? (make-schema :public))))

(deftest test-schema-key
  (are [schema expected]
    (is (= expected (schema-key schema)))
    nil nil
    :public [:public]
    (make-schema :public) [:public]))

(database-test test-read-schemas
  (let [schemas (read-schemas)]
    (is (pos? (count schemas)))
    (is (every? schema? schemas))))

;; TABLES

(deftest test-lookup-table
  (with-frozen-time (now)
    (is (nil? (lookup-table :unknown-table)))
    (let [table (register-table (make-table :applications))]
      (is (= table (lookup-table table)))
      (is (= table (lookup-table :applications)))
      (is (= table (lookup-table :public.applications))))))

(deftest test-make-table
  (let [table (make-table :test [[:id :serial] [:name :text]] :url identity)]
    (is (= :test (:name table)))
    (is (= [:id :name] (:columns table)))
    (is (= identity (:url table))))
  (let [table (make-table :oauth.applications [[:id :serial] [:name :text]] :url identity)]
    (is (= :oauth (:schema table)))
    (is (= :applications (:name table)))
    (is (= [:id :name] (:columns table)))
    (is (= identity (:url table)))))

(deftest test-register-table
  (with-frozen-time (now)
    (let [table (make-table :oauth.applications)]
      (is (= (assoc table :registered-at (now))
             (register-table table)))
      (is (= (assoc table :registered-at (now))
             (get-in @*tables* (table-key table)))))))

(deftest test-table?
  (is (not (table? nil)))
  (is (not (table? "")))
  (is (table? (make-table :continents))))

(deftest test-table-key
  (are [expected table]
    (is (= expected (table-key table)))
    nil nil
    [:public :applications] (make-table :applications)
    [:oauth :applications] (make-table :oauth.applications)))

;; COLUMNS

(deftest test-column?
  (is (not (column? nil)))
  (is (not (column? "")))
  (is (column? (:created-at columns)))
  ;; (is (every? column? (vals (:columns (table :wikipedia.languages)))))
  )

(deftest test-column-key
  (are [expected column]
    (is (= expected (column-key column)))
    nil nil
    [:public :continents :created-at] (:created-at columns)
    [:public :continents :location] (:location columns)))

(deftest test-lookup-column
  (with-frozen-time (now)
    (is (nil? (lookup-column :unknown-column)))
    (let [column (register-column (:created-at columns))]
      (is (= column (lookup-column column)))
      (is (= column (lookup-column :continents/created-at)))
      (is (= column (lookup-column :public.continents/created-at))))))

(deftest test-make-column
  (let [column (:created-at columns)]
    (is (= :public (:schema column)))
    (is (= :continents (:table column)))
    (is (= :created-at (:name column)))
    (is (= :timestamp-with-time-zone (:type column)))
    (is (:native? column))
    (is (nil? (:length column)))
    (is (= "now()" (:default column)))
    (is (:not-null? column)))
  (let [column (:id columns)]
    (is (= :public (:schema column)))
    (is (= :languages (:table column)))
    (is (= :id (:name column)))
    (is (= :serial (:type column)))
    (is (nil? (:length column)))
    (is (:native? column))
    (is (nil? (:default column)))
    (is (:not-null? column)))
  (let [column (:iso-639-1 columns)]
    (is (= :public (:schema column)))
    (is (= :languages (:table column)))
    (is (= :iso-639-1 (:name column)))
    (is (= :varchar (:type column)))
    (is (= 2 (:length column)))
    (is (:native? column))
    (is (nil? (:default column)))
    (is (:not-null? column)))
  (let [column (:location columns)]
    (is (= :public (:schema column)))
    (is (= :continents (:table column)))
    (is (= :location (:name column)))
    (is (= :point-2d (:type column)))
    (is (nil? (:length column)))
    (is (not (:native? column)))
    (is (nil? (:default column)))
    (is (not (:not-null? column)))))

(deftest test-parse-column
  (is (nil? (parse-column :id)))
  (is (= (->Column :public :applications :id)
         (parse-column :applications/id)))
  (is (= (->Column :oauth :applications :id)
         (parse-column :oauth.applications/id)
         (parse-column "oauth.applications/id"))))

(deftest test-register-column
  (with-frozen-time (now)
    (let [column (:created-at columns)]
      (is (= (assoc column :registered-at (now))
             (register-column column)))
      (is (= (assoc column :registered-at (now))
             (get-in @*columns* [:public :continents :created-at]))))))

;; (deftest test-with-ensure-table
;;   (with-ensure-table [languages :wikipedia.languages]
;;     (is (table? languages))
;;     (is (= "languages" (:name languages))))
;;   (with-ensure-table [languages (registry/table :wikipedia.languages)]
;;     (is (table? languages))
;;     (is (= "languages" (:name languages)))))

;; (deftest test-with-ensure-column
;;   (with-ensure-column [:wikipedia.languages [column :id]]
;;     (is (column? column))
;;     (is (= :id (:name column)))
;;     (is (= (registry/table :wikipedia.languages) (:table column))))
;;   (with-ensure-column [(registry/table :wikipedia.languages) [column :id]]
;;     (is (column? column))
;;     (is (= :id (:name column)))
;;     (is (= (registry/table :wikipedia.languages) (:table column)))))
