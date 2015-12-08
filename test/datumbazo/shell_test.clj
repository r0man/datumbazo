(ns datumbazo.shell-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [datumbazo.shell :refer :all]
            [datumbazo.test :refer :all]
            [pallet.common.shell :refer [bash]]))

(deftest test-basename
  (are [args expected]
      (is (= expected (apply basename args)))
    ["/usr/lib/libnetcdf.so"] "libnetcdf.so"
    ["/usr/lib/libnetcdf.so" ".so"] "libnetcdf"
    ["/usr/lib"] "lib"
    ["/usr/"] "usr"
    ["usr"] "usr"
    ["/"] "/"
    ["."] "."
    [".."] ".."))

(deftest test-dirname
  (are [url expected]
      (is (= expected (dirname url)))
    "/usr/lib/libnetcdf.so" "/usr/lib"
    "/usr/lib" "/usr"
    "/usr/" "/"
    "usr" "."
    "/" "/"
    "." "."
    ".." ".."))

(deftest test-exec-checked-script*
  (let [{:keys [err exit out] :as result}
        (exec-checked-script* "echo \"x\"")]
    (is (= 0 exit))
    (is (= "x\n" out))
    (is (= "" err))))

(deftest test-exec-checked-script
  (let [{:keys [err exit out] :as result}
        (exec-checked-script "echo \"x\"" ("echo" "x"))]
    (is (= 0 exit))
    (is (= "echo \"x\"...\nx\n#> echo \"x\" : SUCCESS\n" out))
    (is (= "" err))))

(deftest test-psql
  (is (= 0 (:exit (psql db))))
  (is (= 0 (:exit (psql db {:command "SELECT 1;"}))))
  (spit "/tmp/test-psql.sql" "SELECT 1;" )
  (is (= 0 (:exit (psql db {:file "/tmp/test-psql.sql"}))))
  (is (zero? (:exit (psql db {:command "SELECT 1;"})))))

(deftest test-shp2pgsql
  (with-test-db [db]
    (is (= 0 (:exit (shp2pgsql
                     db :natural-earth.ports
                     "test-resources/ne_10m_ports/ne_10m_ports.dbf"
                     "/tmp/test-shp2pgsql.sql"))))))

(deftest test-raster2pgsql
  (with-test-db [db]
    (with-redefs
      [bash (fn [script]
              (is (= (str "echo 'Running raster2pgsql...';\n{\n    # shell.clj:"
                          "104\nraster2pgsql -c -t auto INPUT weather.nww3-dirp"
                          "wsfc-2013-02-10 > OUTPUT\n } || { echo '#> Running r"
                          "aster2pgsql : FAIL'; exit 1;} >&2 \necho '#> Running"
                          " raster2pgsql : SUCCESS'\n") script))
              {:exit 0})]
      (raster2pgsql db :weather.nww3-dirpwsfc-2013-02-10 "INPUT" "OUTPUT"))))

(deftest test-dump-table
  (with-test-db [db]
    (let [file (io/file "/tmp/test-dump-table.sql" )]
      (is (zero? (:exit (dump-table db :continents file))))
      (is (.exists file))
      (is (.delete file)))))

(deftest test-exec-sql-file
  (with-test-db [db]
    (let [file (io/file "/tmp/test-exec-sql-file.sql" )]
      (dump-table db :countries file)
      (is (zero? (:exit (exec-sql-file db file))))
      (is (.delete file)))))
