(ns database.test.tables
  (:use clojure.test
        ;; database.core
        database.columns
        ;; database.connection
        database.tables
        database.test
        ;; database.protocol
        ;; database.fixtures
        ))

;; (deftest test-key-columns
;;   (are [expected record]
;;     (is (= expected (map :name (key-columns (table :wikipedia.languages) record))))
;;     [] {}
;;     [:id] {:id 1}
;;     [:id :iso-639-1] {:id 1 :iso-639-1 "de"}
;;     [:id :iso-639-1] {:id 1 :iso-639-1 "de" :created-at "2011-12-30"}))

;; (deftest test-table-name
;;   (with-quoted-identifiers \"
;;     (are [table expected]
;;       (is (= expected (table-name table)))
;;       :weather-models "\"weather-models\""
;;       "weather-models" "weather-models") ))

;; (deftest test-as-identifier
;;   (is (= "continents" (as-identifier (make-table :continents))))
;;   (with-quoted-identifiers \"
;;     (is (= "\"continents\"" (as-identifier (make-table :continents)))))
;;   (with-quoted-identifiers \"
;;     (is (= "\"public\".\"continents\"" (as-identifier (make-table :public.continents))))))

;; (deftest test-qualified-table-name
;;   (with-quoted-identifiers \"
;;     (are [table expected]
;;       (is (= expected (qualified-table-name table)))
;;       :weather-models "\"weather-models\""
;;       "weather-models" "weather-models"
;;       ;; (registry/table :photo-thumbnails) "photo-thumbnails"
;;       )))

;; ;; (deftest test-default-columns
;; ;;   (let [columns (default-columns (table :wikipedia.languages))]
;; ;;     (is (every? column? columns))
;; ;;     (is (= [:updated-at :created-at :iso-639-2 :iso-639-1 :family :name :id]
;; ;;            (map :name columns)))))

;; (deftest test-select-columns
;;   [empty? (select-columns (table :wikipedia.languages) [])]
;;   (let [columns (select-columns (table :wikipedia.languages) [:id])]
;;     (is (every? column? columns))
;;     (is (= [:id] (map :name columns)))))

;; ;; (deftest test-primary-key-columns
;; ;;   (is (= [:id] (map :name (primary-key-columns (registry/table :wikipedia.languages))))))

;; ;; (deftest test-unique-columns
;; ;;   (is (= [:iso-639-2 :iso-639-1 :name]
;; ;;          (map :name (unique-columns (registry/table :wikipedia.languages))))))
