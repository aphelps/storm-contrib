(defproject storm/storm-kafka "storm-0.9.0-kafka-0.8.0-scala292"
  :java-source-paths ["src/jvm"]
  :repositories {"scala-tools" "http://scala-tools.org/repo-releases"
                  "conjars" "http://conjars.org/repo/"}
  :dependencies [[org.scala-lang/scala-library "2.9.2"]
                  [kafka/kafka "0.8.0-20130327"
                  :exclusions [org.apache.zookeeper/zookeeper
                               log4j/log4j]]]
  :profiles
  {:provided {:dependencies [[storm "0.9.0-wip15"]
                             ;;[ch.qos.logback/logback-classic "1.0.6"]
                             [org.clojure/clojure "1.4.0"]]}}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :min-lein-version "2.0")
