(ns datumbazo.io-test
  (:require [clj-time.core :refer [date-time]]
            [clojure.test :refer :all]
            [datumbazo.io :as io])
  (:import java.util.Date
           org.joda.time.DateTime
           org.postgresql.util.PGobject))

(deftest test-citext
  (is (nil? (io/citext nil)))
  (is (= (io/citext "x") (io/citext (io/citext "x"))))
  (let [text (io/citext "")]
    (is (instance? PGobject text))
    (is (= "citext" (.getType text)))
    (is (= "" (.getValue text))))
  (let [text (io/citext "Abc")]
    (is (instance? PGobject text))
    (is (= "citext" (.getType text)))
    (is (= "Abc" (.getValue text)))))

(deftest test-read-instant-date-time
  (let [date (Date. 0)
        time (date-time 1970)]
    (is (= "#inst \"1970-01-01T00:00:00.000-00:00\"" (pr-str date)))
    (is (= "#inst \"1970-01-01T00:00:00.000-00:00\"" (pr-str time)))
    (is (= date (read-string (pr-str date))))
    (is (= date (read-string (pr-str time))))
    (is (= date (read-string (pr-str (DateTime. 0)))))
    (binding [*data-readers* {'inst io/read-instant-date-time}]
      (is (= time (read-string (pr-str date))))
      (is (= time (read-string (pr-str time))))
      (is (= time (read-string (pr-str (DateTime. 0))))))))
