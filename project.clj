(defproject database-clj "0.0.3-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :min-lein-version "2.0.0"
  :dependencies [[c3p0/c3p0 "0.9.1.2"]
                 [clj-time "0.4.2"]
                 [geo-clj "0.0.3-SNAPSHOT"]
                 [inflections "0.7.0-SNAPSHOT"]
                 [org.clojars.r0man/korma "0.3.0-beta10"]
                 [migrate/migrate.core "0.0.9-SNAPSHOT"]
                 [org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.postgis/postgis-jdbc "1.3.3"]
                 [postgresql/postgresql "9.1-901.jdbc4"]]
  :profiles {:dev {:dependencies [[validation-clj "0.4.0-SNAPSHOT"]]
                   :resource-paths ["test-resources"]}}
  :plugins [[migrate/migrate.lein "0.0.9-SNAPSHOT"]]
  :migrate [database.fixtures]
  :hooks [environ.leiningen.hooks])
