(defproject datumbazo "0.7.25-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[clj-time "0.9.0"]
                 [com.palletops/stevedore "0.8.0-beta.7"]
                 [com.stuartsierra/component "0.2.3"]
                 [commandline-clj "0.1.7"]
                 [geo-clj "0.3.21"]
                 [inflections "0.9.13"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [org.xerial/sqlite-jdbc "3.8.7"]
                 [mysql/mysql-connector-java "5.1.35"]
                 [slingshot "0.12.2"]
                 [sqlingvo "0.7.10"]]
  :profiles {:1.7 {:dependencies [[org.clojure/clojure "1.7.0-beta1"]]}
             :dev {:dependencies [[ch.qos.logback/logback-classic "1.1.3"]
                                  [com.mchange/c3p0 "0.9.5"]
                                  [com.jolbox/bonecp "0.8.0.RELEASE"]
                                  [validation-clj "0.5.6"]]}
             :test {:resource-paths ["test-resources"]}}
  :aliases {"test-ancient" ["test"]}
  :main ^{:skip-aot true} datumbazo.fixtures)
