(ns datumbazo.test.json
  (:import org.postgis.Point)
  (:use [clojure.data.json :only [read-str write-str]]
        datumbazo.json
        clojure.test))

(deftest test-write-str
  (are [geom expected]
       (is (= expected (read-str (write-str geom))))
       (Point. 1 2)
       {"type" "Point", "coordinates" [1.0 2.0 0.0]}))