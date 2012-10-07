(ns database.util
  (:require [clojure.string :refer [blank? split]]))

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
  (if-let [matches (re-matches #"([^:]+)://(([^:]+):([^@]+)@)?(([^:/]+)(:([0-9]+))?((/([^?]*))(\?(.*))?))" (str s))]
    (let [db (nth matches 11)
          scheme (nth matches 1)
          server-name (nth matches 6)
          server-port (parse-integer (nth matches 8))]
      {:db db
       :host (nth matches 6)
       :password (nth matches 4)
       :port server-port
       :scheme (nth matches 1)
       :server-name server-name
       :server-port server-port
       :subname (str "//" server-name (if server-port (str ":" server-port)) "/" db)
       :subprotocol scheme
       :uri (nth matches 10)
       :user (nth matches 3)
       :params (parse-params (nth matches 13))
       :query-string (nth matches 13)})
    (throw (IllegalArgumentException. (format "Can't parse database url: %s" (str s))))))
