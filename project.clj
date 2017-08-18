(defproject vise890/zookareg "0.1.0-SNAPSHOT"
  :description "Embedded `zo`okeeper `ka`fka and Confluent's Schema `reg`istry"
  :url "http://github.com/vise890/zookareg"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :repositories {"confluent" "http://packages.confluent.io/maven"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [integrant "0.5.0"]
                 [io.confluent/kafka-schema-registry "3.3.0"
                  :exclusions [org.apache.kafka/kafka-clients
                               org.apache.kafka/kafka_2.11]]
                 [org.apache.kafka/kafka_2.11 "0.10.2.1"
                  :exclusions [org.apache.zookeeper/zookeeper]]
                 [org.apache.curator/curator-test "4.0.0"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [me.raynes/fs "1.4.6"]])
