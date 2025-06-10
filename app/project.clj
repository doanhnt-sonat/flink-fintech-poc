(defproject flinkfintechpoc/app "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [metosin/reitit "0.9.1"]
                 [ring/ring-core "1.14.1"]
                 [ring/ring-jetty-adapter "1.14.1"]
                 [com.stuartsierra/component "1.1.0"]
                 [aero "1.1.6"]
                 [metosin/malli "0.18.0"]
                 [hiccup/hiccup "2.0.0-RC5"]
                 [com.kinde/kinde-core "2.0.1"]

                 [camel-snake-kebab "0.4.3"]
                 [org.babashka/http-client "0.4.22"]
                 [com.cnuernber/charred "1.037"]
                 [faker "0.3.2"]

                 [com.github.seancorfield/next.jdbc "1.3.1002"]
                 [com.layerware/hugsql "0.5.3"]
                 [com.github.seancorfield/honeysql "2.7.1310"]
                 [org.postgresql/postgresql "42.7.6"]
                 [org.flywaydb/flyway-core "11.8.2"]
                 [org.flywaydb/flyway-database-postgresql "11.8.2"]

                 [com.zaxxer/HikariCP "6.3.0"]

                 [org.clojure/tools.logging "1.3.0"]
                 [org.slf4j/slf4j-api "2.1.0-alpha1"]
                 [org.slf4j/slf4j-simple "2.1.0-alpha1"]]
  :main ^:skip-aot flinkfintechpoc.app
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
