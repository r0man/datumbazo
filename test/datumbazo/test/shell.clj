(ns datumbazo.test.shell
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
        (exec-checked-script "echo \"x\"" (echo "x"))]
    (is (= 0 exit))
    (is (= "echo x...\nx\n...done\n" out))
    (is (= "" err))))

(database-test test-psql
  (is (= 0 (:exit (psql))))
  (is (= 0 (:exit (psql :command "SELECT 1;"))))
  (spit "/tmp/test-psql.sql" "SELECT 1;" )
  (is (= 0 (:exit (psql :file "/tmp/test-psql.sql"))))
  (with-redefs
    [bash (fn [script]
            (is (= (str "echo \"Running psql...\"\n{ export PGPASS=\"scotch\" && "
                        "psql --command \"SELECT 1;\" --dbname datumbazo --host localhost "
                        "--username tiger --quiet; } || { echo \"Running psql\" failed; exit 1; } "
                        ">&2 \necho \"...done\"\n")
                   script))
            {:exit 0})]
    (psql :command "SELECT 1;")))

(database-test test-shp2pgsql
  (is (= 0 (:exit (shp2pgsql :natural-earth.ports "test-resources/ne_10m_ports/ne_10m_ports.dbf" "/tmp/test-shp2pgsql.sql"))))
  (with-redefs
    [bash (fn [script]
            (is (= (str "echo \"Running shp2pgsql...\"\n{ shp2pgsql -c test-resources/ne_10m_ports/ne_10m_ports.dbf natural_earth.ports"
                        " > /tmp/test-shp2pgsql.sql; } || { echo \"Running shp2pgsql\" failed; exit 1; } >&2 \necho \"...done\"\n")
                   script))
            {:exit 0})]
    (shp2pgsql :natural-earth.ports "test-resources/ne_10m_ports/ne_10m_ports.dbf" "/tmp/test-shp2pgsql.sql")))
