(ns datumbazo.shell
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [blank? join split replace]]
            [clojure.tools.logging :refer [logp]]
            [datumbazo.connection :refer [*connection*]]
            [pallet.common.shell :refer [bash]]
            [pallet.stevedore :refer [checked-script with-script-language]]
            [pallet.stevedore.bash :refer :all]
            [slingshot.slingshot :refer [throw+]]
            [sqlingvo.util :refer [as-identifier]]))

(defn basename
  "Returns the basename of `s`."
  [s & [ext]]
  (replace
   (if (contains? #{"." ".." "/"} s)
     s (last (split s #"/")))
   (re-pattern (str (or ext "") "$")) ""))

(defn dirname
  "Returns the dirname of `s`."
  [s]
  (let [split (remove blank? (split s #"/"))]
    (cond
     (contains? #{"." ".." "/"} s)
     s
     (= 0 (count split))
     "/"
     (and (= 1 (count split))
          (not (= \/ (first s))))
     "."
     (= \/ (first s))
     (str "/" (join "/" (butlast split))))))

(defn log-lines [level lines]
  (doseq [line (split lines #"\n")] (logp level line)))

(defn exec-checked-script* [script]
  (let [result (bash script)]
    (if (pos? (:exit result))
      (throw+ (assoc result :type :exec-checked-script)))
    (doall (map (partial log-lines :debug) (remove nil? (map result [:out :err]))))
    result))

(defmacro exec-checked-script
  "Execute a bash script, throwing an exception if any element of the
  script fails."
  [name & script]
  `(with-script-language :pallet.stevedore.bash/bash
     (exec-checked-script* (checked-script ~name ~@script))))

(defn psql
  "Run the psql command."
  [& {:as opts}]
  (let [opts (merge *connection* opts)]
    (exec-checked-script
     "Running psql"
     ("export" ~(format "PGPASS=\"%s\"" (:password opts)))
     ("psql"
      ~(if-let [command (:command opts)]
         (format "--command \"%s\"" command) "")
      ~(if-let [db (:db opts)]
         (format "--dbname %s" db) "")
      ~(if-let [file (:file opts)]
         (format "--file %s" file) "")
      ~(if-let [host (:server-name opts)]
         (format "--host %s" host) "")
      ~(if-let [port (:server-port opts)]
         (format "--port %s" port) "")
      ~(if-let [username (:username opts)]
         (format "--username %s" username) "")
      "--quiet"))))

(defn shp2pgsql
  "Run the shp2pgsql command."
  [table shape-file sql-file & {:as opts}]
  (exec-checked-script
   "Running shp2pgsql"
   ("shp2pgsql"
    ~(case (:mode opts)
       :append "-a"
       :create "-c"
       :drop "-d"
       :prepare "-p"
       nil "-c")
    ~(if (:index opts)
       "-I" "")
    ~(if-let [srid (:srid opts)]
       (format "-s %s" srid) "")
    ~(if-let [encoding (:encoding opts)]
       (format "-W %s" encoding) "")
    ~shape-file ~(as-identifier table) > ~sql-file)))

(defn raster2pgsql
  "Run the raster2pgsql command."
  [table input output & {:as opts}]
  (exec-checked-script
   "Running shp2pgsql"
   ("raster2pgsql"
    ~(if-let [srid (:srid opts)]
       (format "-s %s" srid) "")
    ~(if-let [band (:band opts)]
       (format "-b %s" band) "")
    ~(case (:mode opts)
       :append "-a"
       :create "-c"
       :drop "-d"
       :prepare "-p"
       nil "-c")
    ~(if-let [column (:column opts)]
       (format "-f %s" column) "")
    ~(if-let [no-data (:no-data opts)]
       (format "-N %s" no-data) "")
    ~(if (:no-transaction opts)
       "-e" "")
    ~(if (:regular-blocking opts)
       "-r" "")
    ~(if (:disable-max-extend opts)
       "-x" "")
    ~(if (:constraints opts)
       "-C" "")
    ~(if (:index opts)
       "-I" "")
    ~(if (:analyze opts)
       "-M" "")
    ~(if (:register opts)
       "-R" "")
    ~(if (sequential? input)
       (join " " input)
       input)
    ~(as-identifier table) > ~output)))
