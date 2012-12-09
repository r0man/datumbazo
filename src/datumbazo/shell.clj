(ns datumbazo.shell
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [blank? join split replace]]
            [clojure.tools.logging :refer [logp]]
            [datumbazo.connection :refer [*connection*]]
            [pallet.common.shell :refer [bash]]
            [pallet.stevedore :refer [checked-script with-script-language]]
            [pallet.stevedore.bash :refer :all]
            [slingshot.slingshot :refer [throw+]]))

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
     (export ~(format "PGPASS=\"%s\"" (:password opts)))
     (psql
      ~(if-let [command (:command opts)]
         (format "--command \"%s\"" command))
      ~(if-let [file (:file opts)]
         (format "--file \"%s\"" file))
      --dbname ~(:db opts)
      --host ~(:server-name opts)
      --port ~(or (:server-port opts) 5432)
      --quiet
      --username ~(or (:username opts) (System/getenv "USER"))))))
