(ns datumbazo.shell-test
  (:require [pallet.common.shell :refer [bash]])
  (:use datumbazo.shell
        datumbazo.test
        clojure.test))

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
  (is (= 0 (:exit (psql db :command "SELECT 1;"))))
  (spit "/tmp/test-psql.sql" "SELECT 1;" )
  (is (= 0 (:exit (psql db :file "/tmp/test-psql.sql"))))
  (with-redefs
    [bash (fn [script]
            (is (= (str "echo 'Running psql...';\n{\n# shell.clj:59\nexport PGPASS=\"scotch\" && \\\n"
                        "# shell.clj:60\npsql --command \"SELECT 1;\" --dbname datumbazo --host localhost --username tiger --quiet\n"
                        " } || { echo '#> Running psql : FAIL'; exit 1;} >&2 \necho '#> Running psql : SUCCESS'\n")
                   script))
            {:exit 0})]
    (psql db :command "SELECT 1;")))

(database-test test-shp2pgsql
  (is (= 0 (:exit (shp2pgsql db :natural-earth.ports "test-resources/ne_10m_ports/ne_10m_ports.dbf" "/tmp/test-shp2pgsql.sql"))))
  (with-redefs
    [bash (fn [script]
            (is (= (str "echo 'Running shp2pgsql...';\n{\n    # shell.clj:80\n"
                        "shp2pgsql -c test-resources/ne_10m_ports/ne_10m_ports.dbf natural_earth.ports > /tmp/test-shp2pgsql.sql\n } "
                        "|| { echo '#> Running shp2pgsql : FAIL'; exit 1;} >&2 \necho '#> Running shp2pgsql : SUCCESS'\n")
                   script))
            {:exit 0})]
    (shp2pgsql db :natural-earth.ports "test-resources/ne_10m_ports/ne_10m_ports.dbf" "/tmp/test-shp2pgsql.sql")))

(database-test test-raster2pgsql
  (with-redefs
    [bash (fn [script]
            (is (= (str "echo 'Running shp2pgsql...';\n{\n    # shell.clj:100\n"
                        "raster2pgsql -c INPUT weather.nww3_dirpwsfc_2013_02_10 > OUTPUT\n } "
                        "|| { echo '#> Running shp2pgsql : FAIL'; exit 1;} >&2 \necho '#> Running shp2pgsql : SUCCESS'\n") script))
            {:exit 0})]
    (raster2pgsql db :weather.nww3-dirpwsfc-2013-02-10 "INPUT" "OUTPUT")))
