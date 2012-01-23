(ns database.util)

(defn parse-int [s]
  (try (Integer/parseInt (str s))
       (catch NumberFormatException _ nil)))