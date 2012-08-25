(defproject storm/trident-kafka "0.0.2-scala-2.9.2-wip4"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo"}
  :dependencies [[org.scala-lang/scala-library "2.9.2"]
                 [com.twitter/kafka_2.9.2 "0.7.0"
                   :exclusions [org.apache.zookeeper/zookeeper
                                log4j/log4j]]]
  :dev-dependencies [[storm "0.8.0"]
                     [org.clojure/clojure "1.4.0"]]
)
