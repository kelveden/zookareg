(ns zookareg.core
  (:require [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [zookareg.state :as state]
            [zookareg.utils :as ut])
  (:import (kafka.admin AdminUtils RackAwareMode$Enforced$)
           (kafka.utils ZKStringSerializer$ ZkUtils)
           (org.I0Itec.zkclient ZkClient ZkConnection)
           (java.util Properties)))

(def default-config
  {:ports        {:kafka           9092
                  :zookeeper       2181
                  :schema-registry 8081}
   :kafka-config {}})

(defn- ->ports [zookeeper kafka schema-registry]
  {:ports {:kafka           kafka
           :zookeeper       zookeeper
           :schema-registry schema-registry}})

(defn- wait-for-topic!
  [^ZkUtils zu topic]
  (deref
    (future
      (loop [exists? false]
        (when (not exists?)
          (do (Thread/sleep 100)
              (recur (AdminUtils/topicExists zu topic))))))
    2000 (log/warn (str "Topic " topic " was not created after 2s."))))

(defn- create-topics
  [topics port]
  (let [host (str "localhost:" port)]
    (with-open [zk (ZkClient. host 1000 1000 (ZKStringSerializer$/MODULE$))]
      (let [zu (ZkUtils. zk (ZkConnection. host) false)]
        (doseq [topic topics]
          (let [topic-name (if (string? topic) topic (:name topic))]
            (AdminUtils/createTopic zu topic-name
                                    (or (:partitions topic) 1)
                                    (or (:replication-factor topic) 1)
                                    (Properties.) (RackAwareMode$Enforced$.))
            (wait-for-topic! zu topic-name)))))))

(defn ->available-ports-config []
  (merge default-config
         (->ports (ut/->available-port)
                  (ut/->available-port)
                  (ut/->available-port))))

(defn ->ig-config [config]
  {:zookareg.schema-registry/schema-registry
   {:ports      (:ports config)
    :_kafka     (ig/ref :zookareg.kafka/kafka)
    :_zookeeper (ig/ref :zookareg.zookeeper/zookeeper)}

   :zookareg.kafka/kafka
   {:ports        (:ports config)
    :kafka-config (:kafka-config config)
    :_zookeeper   (ig/ref :zookareg.zookeeper/zookeeper)}

   :zookareg.zookeeper/zookeeper
   {:ports (:ports config)}})

(defn halt-zookareg! []
  (when @state/state
    (swap! state/state
           (fn [s]
             (ig/halt! (:system s))
             nil))))

(defn init-zookareg
  ([] (init-zookareg default-config))
  ([{:keys [topics] :as config}]
   (let [ig-config (->ig-config config)
         config-pp (with-out-str (pprint/pprint config))]
     (log/info "starting ZooKaReg with config:" config-pp)
     (try
       (halt-zookareg!)
       (ig/load-namespaces ig-config)
       ;; TODO stick in the same atom!
       (reset! state/state
               {:system (ig/init ig-config)
                :config ig-config})

       (when-not (empty? topics)
         (create-topics topics (get-in config [:ports :zookeeper])))

       (catch clojure.lang.ExceptionInfo ex
         ;; NOTE tears down partially initialised system
         (ig/halt! (:system (ex-data ex)))
         (throw (.getCause ex)))))))

(defn with-zookareg-fn
  "Starts up zookareg with the specified configuration; executes the function then shuts down."
  ([config f]
   {:pre [(map? config) (fn? f)]}
   (try
     (init-zookareg config)
     (f)
     (finally
       (halt-zookareg!))))
  ([f]
   (with-zookareg-fn default-config f)))

(defmacro with-zookareg
  "Starts up zookareg with the specified configuration; executes the body then shuts down."
  [config & body]
  `(with-zookareg-fn ~config (fn [] ~@body)))
