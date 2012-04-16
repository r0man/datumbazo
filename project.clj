(defproject database-clj "0.0.2-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :min-lein-version "2.0.0"
  :dependencies [[c3p0/c3p0 "0.9.1.2"]
                 [clj-time "0.4.0"]
                 [geo-clj "0.0.3-SNAPSHOT"]
                 [inflections "0.7.0-SNAPSHOT"]
                 [korma "0.3.0-beta6"]
                 [migrate "0.0.7-SNAPSHOT"]
                 [org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.1.4"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.postgis/postgis-jdbc "1.3.3"]
                 [org.slf4j/slf4j-log4j12 "1.5.6"]
                 [postgresql/postgresql "9.1-901.jdbc4"]]
  :profiles {:dev {:dependencies [[validation-clj "0.2.1-SNAPSHOT"]]
                   :resource-paths ["test-resources"]}}
  :plugins [[migrate "0.0.7-SNAPSHOT"]]
  :migrate [database.fixtures])
