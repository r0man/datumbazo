(defproject datumbazo "0.5.26-SNAPSHOT"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :lein-release {:deploy-via :clojars}
  :dependencies [[clj-time "0.6.0"]
                 [com.palletops/stevedore "0.8.0-beta.6"]
                 [com.stuartsierra/component "0.2.1"]
                 [commandline-clj "0.1.7"]
                 [environ "0.4.0"]
                 [geo-clj "0.3.9"]
                 [inflections "0.9.5"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [org.clojure/tools.logging "0.2.6"]
                 [postgresql/postgresql "9.1-901-1.jdbc4"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [mysql/mysql-connector-java "5.1.29"]
                 [slingshot "0.10.3"]
                 [sqlingvo "0.5.15"]]
  :profiles {:dev {:dependencies [[validation-clj "0.5.6"]
                                  [org.slf4j/slf4j-log4j12 "1.7.6"]
                                  [c3p0/c3p0 "0.9.1.2"]
                                  [com.jolbox/bonecp "0.8.0.RELEASE"]]
                   :resource-paths ["test-resources"]
                   :env {:test-db "postgresql://tiger:scotch@localhost/datumbazo"}}}
  :plugins [[lein-environ "0.4.0"]]
  :main ^{:skip-aot true} datumbazo.fixtures
  :db [{:name :test-db
        :fixtures "db/test-db/fixtures"
        :migrations "db/test-db/migrations"}])
