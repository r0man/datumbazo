(ns database.util
  (:import java.io.File)
  (:require [clojure.java.io :refer [file]]
            [clojure.string :refer [blank? split]]))

(defn clojure-file?
  "Returns true if `path` is a fixture file, otherwise false."
  [path]
  (and (.isFile (file path))
       (re-matches #"(?i).*\.cljs?$" (str path))))

(defn file-split
  "Split `s` at the file separator."
  [s] (split (str s) (re-pattern File/separator)))

(defmacro defn-memo
  "Just like defn, but memoizes the function using clojure.core/memoize"
  [fn-name & defn-stuff]
  `(do
     (defn ~fn-name ~@defn-stuff)
     (alter-var-root (var ~fn-name) memoize)
     (var ~fn-name)))

(defn parse-integer
  "Parse `s` as an integer."
  [s]
  (try (Integer/parseInt (str s))
       (catch NumberFormatException _ nil)))

(defn parse-params
  "Parse `s` as a query string and return a hash map."
  [s] (->> (split (or s "") #"&")
           (remove blank?)
           (map #(split %1 #"="))
           (mapcat #(vector (keyword (first %1)) (second %1)))
           (apply hash-map)))

(defn parse-url
  "Parse `s` as a database url and return a JDBC/Ring compatible map."
  [s]
  (if-let [matches (re-matches #"(jdbc:)?([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))" (str s))]
    (let [db (nth matches 12)
          scheme (nth matches 2)
          server-name (nth matches 7)
          server-port (parse-integer (nth matches 9))]
      {:db db
       :host (nth matches 7)
       :password (nth matches 5)
       :port server-port
       :scheme scheme
       :server-name server-name
       :server-port server-port
       :subname (str "//" server-name (if server-port (str ":" server-port)) "/" db)
       :subprotocol scheme
       :uri (nth matches 11)
       :user (nth matches 4)
       :params (parse-params (nth matches 14))
       :query-string (nth matches 14)})))
