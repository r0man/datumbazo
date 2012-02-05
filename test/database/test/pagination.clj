(ns database.test.pagination
  (:use [korma.core :exclude (join table offset)]
        database.pagination
        database.test
        database.test.examples
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
