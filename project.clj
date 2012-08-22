(defproject storm/trident-kafka "0.0.2-wip3"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[storm/kafka "0.7.0-incubating"
                   :exclusions [org.apache.zookeeper/zookeeper
                                log4j/log4j]]]
  :dev-dependencies [[storm "0.8.0"]
                     [org.clojure/clojure "1.4.0"]]
)
