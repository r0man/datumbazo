(defproject datumbazo "0.15.0"
  :description "Clojure Database Kung-Foo"
  :url "https://github.com/r0man/datumbazo"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[clj-time "0.15.2"]
                 [com.palletops/stevedore "0.8.0-beta.7"]
                 [com.stuartsierra/component "1.0.0"]
                 [commandline-clj "0.3.0"]
                 [geo-clj "0.6.3"]
                 [inflections "0.13.2"]
                 [org.clojure/clojure "1.10.2"]
                 [org.clojure/data.json "1.0.0"]
                 [org.clojure/tools.logging "1.1.0"]
                 [postgis.spec "0.1.4"]
                 [potemkin "0.4.5"]
                 [sqlingvo "0.9.35"]]
  :plugins [[jonase/eastwood "0.3.13"]]
  :aliases {"ci" ["do" ["test"] ["lint"]]
            "lint" ["do"  ["eastwood"]]}
  :eastwood {:exclude-linters [:implicit-dependencies
                               :unused-ret-vals]}
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [org.clojure/test.check "1.1.0"]]
                   :repl-options {:init-ns user}
                   :resource-paths ["test-resources"]}
             :provided {:dependencies [[com.jolbox/bonecp "0.8.0.RELEASE"]
                                       [com.mchange/c3p0 "0.9.5.5"]
                                       [com.zaxxer/HikariCP "4.0.1"]
                                       [funcool/clojure.jdbc "0.9.0"]
                                       [mysql/mysql-connector-java "8.0.23"]
                                       [net.postgis/postgis-jdbc "2.5.0" :exclusions [postgresql org.postgresql/postgresql]]
                                       [org.clojure/java.jdbc "0.7.11"]
                                       [org.postgresql/postgresql "42.2.18"]
                                       [org.xerial/sqlite-jdbc "3.34.0"]
                                       [seancorfield/next.jdbc "1.1.613"]]}}
  :main ^{:skip-aot true} datumbazo.fixtures)
