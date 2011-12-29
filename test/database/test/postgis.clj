(ns database.test.postgis
  (:use clojure.test
        database.core
        database.columns
        database.postgis
        database.test
        database.test.examples))

(def point-2d (make-column :point_2d [:point-2d]))
(def multipolygon-2d (make-column :multipolygon_2d [:multipolygon-2d]))

(database-test test-add-column
  (let [table (create-table test-table)]
    (is (= point-2d (add-column table point-2d)))
    (is (= multipolygon-2d (add-column table multipolygon-2d)))))

(database-test test-add-geometry-column
  (let [table (create-table test-table)]
    (is (= point-2d (add-geometry-column table point-2d 4326 "POINT" 2)))
    (is (= multipolygon-2d (add-geometry-column table multipolygon-2d 4326 "MULTIPOLYGON" 2)))))
