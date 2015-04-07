(ns datumbazo.shell
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [blank? join split replace]]
            [clojure.tools.logging :refer [logp]]
            [datumbazo.db :refer [parse-url]]
            [inflections.core :refer [underscore]]
            [pallet.common.shell :refer [bash]]
            [pallet.stevedore :refer [checked-script with-script-language]]
            [pallet.stevedore.bash :refer :all]
            [slingshot.slingshot :refer [throw+]]
            [sqlingvo.core :refer [sql-name]]))

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
  [db & {:as opts}]
  (let [opts (merge db opts)]
    (exec-checked-script
     "Running psql"
     ("export" ~(format "PGPASSWORD=\"%s\"" (:password opts)))
     ("psql"
      ~(if-let [command (:command opts)]
         (format "--command \"%s\"" command) "")
      ~(if-let [db (:name opts)]
         (format "--dbname %s" db) "")
      ~(if-let [file (:file opts)]
         (format "--file %s" file) "")
      ~(if-let [host (:host opts)]
         (format "--host %s" host) "")
      ~(if-let [port (:port opts)]
         (format "--port %s" port) "")
      ~(if-let [username (:user opts)]
         (format "--username %s" username) "")
      "--quiet"))))

(defn shp2pgsql
  "Run the shp2pgsql command."
  [db table shape-file sql-file & {:as opts}]
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
    ~shape-file ~(sql-name db table) > ~(str sql-file))))

(defn raster2pgsql
  "Run the raster2pgsql command."
  [db table input output & {:as opts}]
  (let [{:keys [width height]} opts]
    (exec-checked-script
     "Running raster2pgsql"
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
      ~(if (and width height)
         (format "-t %sx%s" width height) "-t auto")
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
         (join " " (map str input))
         (str input))
      ~(sql-name db table) > ~(str output)))))
