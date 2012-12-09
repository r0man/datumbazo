(ns datumbazo.shell
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [blank? join split replace]]
            [clojure.tools.logging :refer [logp]]
            [pallet.common.shell :refer :all]
            [pallet.stevedore :refer :all]
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

(defmacro with-bash [& body]
  `(with-script-language :pallet.stevedore.bash/bash
     ~@body))

(defn exec-checked-script* [script]
  (let [result (bash script)]
    (if (pos? (:exit result))
      (throw+ (assoc result :type :exec-checked-script)))
    (doall (map (partial log-lines :debug) (map result [:out :err])))
    result))

(defmacro exec-checked-script
  "Execute a bash script, throwing if any element of the script fails."
  [name & script]
  `(with-bash
     (exec-checked-script*
      (checked-script ~name ~@script))))
