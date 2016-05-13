(defproject datumbazo "0.10.1-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[clj-time "0.11.0"]
                 [com.palletops/stevedore "0.8.0-beta.7"]
                 [com.stuartsierra/component "0.3.1"]
                 [commandline-clj "0.2.1"]
                 [geo-clj "0.5.1"]
                 [inflections "0.12.2"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [potemkin "0.4.3"]
                 [prismatic/schema "1.1.1"]
                 [slingshot "0.12.2"]
                 [sqlingvo "0.8.11"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.1.7"]
                                  [com.jolbox/bonecp "0.8.0.RELEASE"]
                                  [com.mchange/c3p0 "0.9.5.2"]
                                  [com.zaxxer/HikariCP "2.4.6"]
                                  [funcool/clojure.jdbc "0.8.0"]
                                  [mysql/mysql-connector-java "5.1.38"]
                                  [org.clojure/java.jdbc "0.6.1"]
                                  [org.postgresql/postgresql "9.4.1208"]
                                  [org.xerial/sqlite-jdbc "3.8.11.2"]
                                  [validation-clj "0.5.6"]]
                   :repl-options {:init-ns user}
                   :resource-paths ["test-resources"]}}
  :aliases {"test-ancient" ["test"]}
  :main ^{:skip-aot true} datumbazo.fixtures)
