(ns database.registry
  (:use [database.tables :only (table-keyword)]))

(defonce ^:dynamic *tables* (atom {}))

(defn find-table
  "Find the database table in the registry by it's name."
  [name] (get @*tables* (table-keyword {:name name})))

(defn register-table
  "Register the database table in the registry."
  [table] (swap! *tables* assoc (table-keyword table) table))
