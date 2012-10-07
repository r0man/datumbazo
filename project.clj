(defproject database-clj "0.0.5-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :min-lein-version "2.0.0"
  :dependencies [[c3p0/c3p0 "0.9.1.2"]
                 [clj-time "0.4.4"]
                 [environ "0.3.0"]
                 [inflections "0.7.3"]
                 [org.clojure/algo.monads "0.1.0"]
                 [org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.postgis/postgis-jdbc "1.3.3"]
                 [postgresql/postgresql "9.1-901.jdbc4"]
                 [ragtime/ragtime.sql.files "0.3.1"]
                 [slingshot "0.10.3"]]
  :profiles {:dev {:dependencies [[validation-clj "0.4.0-SNAPSHOT"]
                                  [org.slf4j/slf4j-log4j12 "1.6.4"]]
                   :resource-paths ["test-resources"]
                   :env {:test-db "postgresql://localhost/test"}}}
  :plugins [[environ/environ.lein "0.3.0"]
            [ragtime/ragtime.lein "0.3.1"]]
  :hooks [environ.leiningen.hooks]
  :ragtime {:database "jdbc:postgresql://localhost/test"
            :migrations ragtime.sql.files/migrations}
  :db [{:name :test-db
        :fixtures "db/fixtures/test-db"
        :migrations "db/migrations/test-db"}])
