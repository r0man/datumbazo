(ns datumbazo.vendor-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :as sql]
            [datumbazo.vendor :as vendor]))

(def urls
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :oracle "oracle://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost:5432/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.sqlite"
   :sqlserver "sqlserver://tiger:scotch@localhost/datumbazo"
   :vertica "vertica://tiger:scotch@localhost/datumbazo"})

(deftest test-jdbc-url
  (are [url expected] (= (vendor/jdbc-url (sql/db url)) expected)
    (:mysql urls)
    "jdbc:mysql://localhost/datumbazo"
    (:oracle urls)
    "jdbc:oracle::tiger/scotch@localhost:datumbazo"
    (:postgresql urls)
    "jdbc:postgresql://localhost:5432/datumbazo"
    (:sqlite urls)
    "jdbc:sqlite://tmp/datumbazo.sqlite"
    (:sqlserver urls)
    "jdbc:sqlserver://localhost;database=datumbazo;user=tiger;password=scotch"
    (:vertica urls)
    "jdbc:vertica://localhost/datumbazo"
    (str (:vertica urls) "?ssl=true")
    "jdbc:vertica://localhost/datumbazo?ssl=true"))

(deftest test-subname
  (are [url expected] (= (vendor/subname (sql/db url)) expected)
    (:mysql urls)
    "//localhost/datumbazo"
    (:oracle urls)
    ":tiger/scotch@localhost:datumbazo"
    (:postgresql urls)
    "//localhost:5432/datumbazo"
    (:sqlite urls)
    "//tmp/datumbazo.sqlite"
    (:sqlserver urls)
    "//localhost;database=datumbazo;user=tiger;password=scotch"
    (:vertica urls)
    "//localhost/datumbazo"))
