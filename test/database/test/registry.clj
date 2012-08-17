(ns database.test.registry
  (:use clojure.test
        database.columns
        ;; database.tables
        ;; database.fixtures
        database.registry
        database.schema
        database.tables))

(deftest test-schema-key
  (is (= [:public] (schema-key (make-schema :public)))))

(deftest test-table-key
  (is (= [:public :continents] (table-key (make-table :continents))))
  (is (= [:oauth :applications] (table-key (make-table :oauth.applications)))))

(deftest test-column-key
  (is (= [:public :continents :id] (column-key (make-column :continents/id :serial))))
  (is (= [:oauth :applications :id] (column-key (make-column :oauth.applications/id :serial)))))

(deftest test-table
  (let [table (make-table :photo-thumbnails)]
    (register-table table)
    (is (table? (table :photo-thumbnails)))
    (is (= table (table :photo-thumbnails)))
    (is (= table (table 'photo-thumbnails)))
    (is (= table (table "photo-thumbnails")))
    (is (= table (table (table :photo-thumbnails))))))

;; (deftest test-register-table
;;   (let [table (make-table :photo-thumbnails)]
;;     (is (= table (register-table table)))
;;     (is (= table (:photo-thumbnails @*tables*)))))

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
