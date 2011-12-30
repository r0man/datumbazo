(defproject database "0.0.1-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :dependencies [[c3p0/c3p0 "0.9.1.2"]
                 [inflections "0.6.5-SNAPSHOT"]
                 [lein-env "0.0.1-SNAPSHOT"]
                 [org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.0.7"]
                 [org.clojure/tools.logging "0.2.3"]
                 ;; [org.postgis/postgis-jdbc "1.3.3"]
                 [org.slf4j/slf4j-log4j12 "1.5.6"]
                 [postgresql/postgresql "9.1-901.jdbc4"]])
