(ns database.registry)

(defonce ^:dynamic *tables* (atom {}))

(defn find-table
  "Find the database table in the registry by name."
  [name] (get @*tables* (keyword (clojure.core/name name))))

(defn register-table
  "Register the database table in the registry."
  [table] (swap! *tables* assoc (:name table) table))
