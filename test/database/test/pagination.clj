(ns database.test.pagination
  (:use database.fixtures
        database.pagination
        database.test
        clojure.test))

(database-test test-paginate
  (let [german (save-language german) spanish (save-language spanish)]
    (let [result (paginate (languages))]
      (is (= [german spanish] result))
      (let [meta (meta result)]
        (is (= 1 (:page meta)))
        (is (= *per-page* (:per-page meta)))
        (is (= 2 (:total meta)))))
    (let [result (paginate (languages) :page 1 :per-page 1)]
      (is (= [german] result))
      (let [meta (meta result)]
        (is (= 1 (:page meta)))
        (is (= 1 (:per-page meta)))
        (is (= 2 (:total meta)))))
    (let [result (paginate (languages) :page 2 :per-page 1)]
      (is (= [spanish] result))
      (let [meta (meta result)]
        (is (= 2 (:page meta)))
        (is (= 1 (:per-page meta)))
        (is (= 2 (:total meta)))))
    (let [result (paginate (languages-by-family (:family german)) :page 2 :per-page 1)]
      (is (= [spanish] result))
      (let [meta (meta result)]
        (is (= 2 (:page meta)))
        (is (= 1 (:per-page meta)))
        (is (= 2 (:total meta)))))))

(database-test test-paginate*
  (let [german (save-language german) spanish (save-language spanish)]
    (let [result (paginate* (languages-by-family* (:family german)) :page 1 :per-page 2)]
      (is (= [german spanish] result)))))

(deftest test-parse-page
  (is (= *page* (parse-page nil)))
  (is (= *page* (parse-page "")))
  (is (= *page* (parse-page "x")))
  (is (= 2 (parse-page "2"))))

(deftest test-parse-per-page
  (is (= *per-page* (parse-per-page nil)))
  (is (= *per-page* (parse-per-page "")))
  (is (= *per-page* (parse-per-page "x")))
  (is (= 100 (parse-per-page "100"))))

(deftest test-wrap-pagination
  ((wrap-pagination
    (fn [_]
      (is (= 2 *page*))
      (is (= 100 *per-page*))))
   {:params {:page "2" :per-page "100"}})
  ((wrap-pagination
    (fn [_]
      (is (= 2 *page*))
      (is (= 100 *per-page*)))
    :custom-page :custom-per-page)
   {:params {:custom-page "2" :custom-per-page "100"}}))
