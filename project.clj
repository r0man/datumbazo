(defproject datumbazo "0.11.1"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[clj-time "0.12.0"]
                 [com.palletops/stevedore "0.8.0-beta.7"]
                 [com.stuartsierra/component "0.3.1"]
                 [commandline-clj "0.3.0"]
                 [geo-clj "0.6.1"]
                 [inflections "0.12.2"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [potemkin "0.4.3"]
                 [prismatic/schema "1.1.3"]
                 [slingshot "0.12.2"]
                 [sqlingvo "0.8.17"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.1.7"]
                                  [com.jolbox/bonecp "0.8.0.RELEASE"]
                                  [com.mchange/c3p0 "0.9.5.2"]
                                  [com.zaxxer/HikariCP "2.4.7"]
                                  [funcool/clojure.jdbc "0.9.0"]
                                  [mysql/mysql-connector-java "5.1.38"]
                                  [org.clojure/java.jdbc "0.6.1"]
                                  [org.postgresql/postgresql "9.4.1209"]
                                  [net.postgis/postgis-jdbc "2.2.1"
                                   :exclusions [postgresql org.postgresql/postgresql]]
                                  [org.xerial/sqlite-jdbc "3.8.11.2"]
                                  [validation-clj "0.5.6"]]
                   :repl-options {:init-ns user}
                   :resource-paths ["test-resources"]}}
  :aliases {"test-ancient" ["test"]}
  :main ^{:skip-aot true} datumbazo.fixtures)
