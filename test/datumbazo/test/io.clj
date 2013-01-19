(ns datumbazo.test.io
  (:import java.util.Date
           org.postgis.Point
           org.postgis.PGgeometry
           org.joda.time.DateTime)
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.io
        clojure.test))

(deftest test-read-wkt
  (are [s expected]
       (is (= expected (read-wkt s)))
       "POINT(1 2)" (PGgeometry. (Point. 1 2))))

(deftest test-pr-str
  (are [s expected]
       (is (= expected (pr-str s)))
       (Point. 1 2) "#wkt \"POINT(1 2)\""
       (PGgeometry. (Point. 1 2)) "#wkt \"POINT(1 2)\""))

(deftest test-read-instant-date-time
  (let [date (Date. 0)
        time (DateTime. 0)]
    (is (= "#inst \"1970-01-01T00:00:00.000-00:00\"" (pr-str date)))
    (is (= "#inst \"1970-01-01T00:00:00.000-00:00\"" (pr-str time)))
    (is (= date (read-string (pr-str date))))
    (is (= date (read-string (pr-str time))))
    (binding [*data-readers* {'inst read-instant-date-time}]
      (is (= time (read-string (pr-str date))))
      (is (= time (read-string (pr-str time)))))))
