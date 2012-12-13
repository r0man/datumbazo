(ns datumbazo.test.postgis
  (:import org.postgis.Point)
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.postgis
        datumbazo.test
        clojure.test))

(deftest test-read-wkt
  (are [s expected]
       (is (= expected (read-wkt s)))
       "POINT(1 2)" (Point. 1 2)))