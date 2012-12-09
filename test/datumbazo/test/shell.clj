(ns datumbazo.test.shell
  (:use datumbazo.shell
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
