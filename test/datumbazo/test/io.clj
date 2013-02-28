(ns datumbazo.test.io
  (:import java.util.Date
           org.joda.time.DateTime
           org.joda.time.DateTimeZone)
  (:require [clojure.java.jdbc :as jdbc]
            [clj-time.core :refer [date-time]])
  (:use datumbazo.io
        clojure.test))

(deftest test-read-instant-date-time
  (let [date (Date. 0)
        time (date-time 1970)]
    (is (= "#inst \"1970-01-01T00:00:00.000-00:00\"" (pr-str date)))
    (is (= "#inst \"1970-01-01T00:00:00.000-00:00\"" (pr-str time)))
    (is (= date (read-string (pr-str date))))
    (is (= date (read-string (pr-str time))))
    (is (= date (read-string (pr-str (DateTime. 0)))))
    (binding [*data-readers* {'inst read-instant-date-time}]
      (is (= time (read-string (pr-str date))))
      (is (= time (read-string (pr-str time))))
      (is (= time (read-string (pr-str (DateTime. 0))))))))
