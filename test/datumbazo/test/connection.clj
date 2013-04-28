(ns datumbazo.test.connection
  (:import [com.jolbox.bonecp BoneCPDataSource ConnectionHandle]
           com.mchange.v2.c3p0.ComboPooledDataSource
           com.mchange.v2.c3p0.impl.NewProxyConnection
           java.sql.Connection)
  (:require [clojure.java.jdbc :as jdbc])
  (:use datumbazo.connection
        datumbazo.util
        datumbazo.test
        clojure.test))

(def test-url (connection-url :test-db))

(deftest test-connection-spec
  (let [spec (connection-spec "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (= "mysql" (:adapter spec)))
    (is (= "com.mysql.jdbc.Driver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:database spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {:profileSQL "true"} (:params spec)))
    (let [spec (:spec spec)]
      (is (= "mysql" (:subprotocol spec)))
      (is (= "//localhost/datumbazo?profileSQL=true" (:subname spec)))
      (is (= "tiger" (:user spec))) ; MySQL needs :user key
      (is (= "scotch" (:password spec)))))
  (let [spec (connection-spec "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (= "postgresql" (:adapter spec)))
    (is (= "org.postgresql.Driver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:database spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {:ssl "true"} (:params spec)))
    (let [spec (:spec spec)]
      (is (= "postgresql" (:subprotocol spec)))
      (is (= "//localhost:5432/datumbazo?ssl=true" (:subname spec)))
      (is (= "tiger" (:user spec)))
      (is (= "scotch" (:password spec)))))
  (let [spec (connection-spec "sqlite://tmp/datumbazo.sqlite")]
    (is (= "sqlite" (:adapter spec)))
    (is (= "org.sqlite.JDBC" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= {} (:params spec)))
    (let [spec (:spec spec)]
      (is (= "sqlite" (:subprotocol spec)))
      (is (= "//tmp/datumbazo.sqlite" (:subname spec)))))
  (let [spec (connection-spec "sqlite:datumbazo.sqlite")]
    (is (= "sqlite" (:adapter spec)))
    (is (= "org.sqlite.JDBC" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= {} (:params spec)))
    (let [spec (:spec spec)]
      (is (= "sqlite" (:subprotocol spec)))
      (is (= "datumbazo.sqlite" (:subname spec)))))
  (let [spec (connection-spec "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (= "mssql" (:adapter spec)))
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:database spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {} (:params spec)))
    (let [spec (:spec spec)]
      (is (= "sqlserver" (:subprotocol spec)))
      (is (= "//localhost;database=datumbazo;user=tiger;password=scotch" (:subname spec)))))
  (let [spec (connection-spec "oracle://tiger:scotch@localhost/datumbazo")]
    (is (= "oracle" (:adapter spec)))
    (is (= "oracle.jdbc.driver.OracleDriver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:database spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {} (:params spec)))
    (let [spec (:spec spec)]
      (is (= "oracle:thin" (:subprotocol spec)))
      (is (= ":tiger/scotch@localhost:datumbazo" (:subname spec))))))

(database-test test-connection-url
  (is (thrown? IllegalArgumentException (connection-url :unknown-db)))
  (is (= "postgresql://tiger:scotch@localhost/datumbazo" (connection-url :test-db))))

(database-test test-connection
  (let [connection (connection test-url)]
    (is (map? (:spec connection)))
    (is (not (= connection (connection test-url))))))

(database-test test-cached-connection
  (let [connection (cached-connection test-url)]
    (is (map? (:spec connection)))
    (is (= connection (cached-connection test-url)))))

(deftest test-with-connection
  (with-connection [connection db]
    (is (map? connection))
    (is (= 0 (:level connection)))
    (is (instance? java.sql.Connection (:connection connection)))
    (is (= db (:connection-string connection)))))
