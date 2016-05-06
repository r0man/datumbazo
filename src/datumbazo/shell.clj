(ns datumbazo.shell
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [blank? join split replace]]
            [clojure.tools.logging :refer [logp]]
            [datumbazo.util :refer [parse-url]]
            [inflections.core :refer [underscore]]
            [pallet.common.shell :refer [bash]]
            [pallet.stevedore :refer [checked-script with-script-language]]
            [pallet.stevedore.bash :refer :all]
            [slingshot.slingshot :refer [throw+]]
            [sqlingvo.util :refer [sql-name]]
            [clojure.tools.logging :as log]))

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
     (let [script# (checked-script ~name ~@script)]
       (log/debug "Executing script:")
       (log/debug script#)
       (exec-checked-script* script#))))

(defn psql
  "Run the psql command."
  [db & [opts]]
  (exec-checked-script
   "Running psql"
   ("export" ~(format "PGPASSWORD=\"%s\"" (:password db)))
   ("psql"
    ~(if-not (= (:on-error-stop opts) false)
       "--variable ON_ERROR_STOP=1" "" )
    ~(if-let [command (:command opts)]
       (format "--command '%s'" command) "")
    ~(if-let [db (:name db)]
       (format "--dbname %s" db) "")
    ~(if-let [file (:file opts)]
       (format "--file %s" (str file)) "")
    ~(if-let [host (:host db)]
       (format "--host %s" host) "")
    ~(if-let [port (:port db)]
       (format "--port %s" port) "")
    ~(if-let [user (:user db)]
       (format "--username %s" user) "")
    ~(if (:single-transaction opts)
       "--single-transaction" "")
    "--quiet")))

(defn shp2pgsql
  "Run the shp2pgsql command."
  [db table shape-file sql-file & [opts]]
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
  [db table input output & [opts]]
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
      ~(if (:padding opts)
         "-P" "")
      ~(if (:register opts)
         "-R" "")
      ~(if (sequential? input)
         (join " " (map str input))
         (str input))
      ~(sql-name db table) > ~(str output)))))

(defn pg-dump
  "Run the pgdump command."
  [db & [opts]]
  (exec-checked-script
   "Running pgdump"
   ("export" ~(format "PGPASSWORD=\"%s\"" (:password db)))
   ("pg_dump"
    ~(if-let [db-name (:name db)]
       (format "--dbname %s" db-name) "")
    ~(if-let [host (:host db)]
       (format "--host %s" host) "")
    ~(if-let [port (:port db)]
       (format "--port %s" port) "")
    ~(if-let [user (:user db)]
       (format "--username %s" user) "")
    ~(if (:clean opts)
       "--clean" "")
    ~(if (:create opts)
       "--create" "")
    ~(if (:data-only opts)
       "--data-only" "")
    ~(if (:schema-only opts)
       "--schema-only" "")
    ~(if (:disable-triggers opts)
       "--disable-triggers" "")
    ~(if-let [schema (:schema db)]
       (format "--schema %s" schema) "")
    ~(if-let [exclude-schema (:exclude-schema db)]
       (format "--exclude-schema %s" exclude-schema) "")
    ~(if-let [table (:table opts)]
       (format "--table %s" (name table)) "")
    ~(if-let [file (:file opts)]
       (format "--file '%s'" (str file)) ""))))

(defn dump-table
  "Export a database table via pgdump."
  [db table filename & [opts]]
  (->> {:clean (:clean opts true)
        :disable-triggers (:disable-triggers opts true)
        :file filename
        :table table}
       (merge opts)
       (pg-dump db)))

(defn exec-sql-file
  "Execute the SQL in `filename` in database `db`."
  [db filename & [opts]]
  (psql db :file filename))
