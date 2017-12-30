(defproject datumbazo "0.13.10-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[clj-time "0.14.2"]
                 [com.palletops/stevedore "0.8.0-beta.7"]
                 [com.stuartsierra/component "0.3.2"]
                 [commandline-clj "0.3.0"]
                 [geo-clj "0.6.2"]
                 [inflections "0.13.0"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.4.0"]
                 [postgis.spec "0.1.2"]
                 [potemkin "0.4.4"]
                 [sqlingvo "0.9.16"]]
  :plugins [[jonase/eastwood "0.2.5"]
            [lein-difftest "2.0.0"]]
  :aliases {"ci" ["do" ["difftest"] ["lint"]]
            "lint" ["do"  ["eastwood"]]}
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [com.jolbox/bonecp "0.8.0.RELEASE"]
                                  [com.mchange/c3p0 "0.9.5.2"]
                                  [com.zaxxer/HikariCP "2.7.4"]
                                  [funcool/clojure.jdbc "0.9.0"]
                                  [mysql/mysql-connector-java "5.1.44"]
                                  [org.clojure/java.jdbc "0.7.5"]
                                  [org.clojure/test.check "0.9.0"]
                                  [org.postgresql/postgresql "42.1.4"]
                                  [net.postgis/postgis-jdbc "2.2.1"
                                   :exclusions [postgresql org.postgresql/postgresql]]
                                  [org.xerial/sqlite-jdbc "3.21.0.1"]
                                  [validation-clj "0.5.6"]]
                   :repl-options {:init-ns user}
                   :resource-paths ["test-resources"]}}
  :main ^{:skip-aot true} datumbazo.fixtures)
