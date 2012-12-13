(ns datumbazo.test.io
  (:import org.postgis.Point
           org.postgis.PGgeometry)
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.io
        clojure.test))

(deftest test-read-wkt
  (are [s expected]
       (is (= expected (read-wkt s)))
       "POINT(1 2)" (Point. 1 2)))

(deftest test-pr-str
  (are [s expected]
       (is (= expected (pr-str s)))
       (Point. 1 2) "#wkt \"POINT(1 2)\""
       (PGgeometry. (Point. 1 2)) "#wkt \"POINT(1 2)\""))