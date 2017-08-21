(ns zookareg.core
  (:require [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [zookareg.state :as state]
            [zookareg.utils :as ut]))

(def default-ports
  {:kafka           9092
   :zookeeper       2181
   :schema-registry 8081})

(defn ->ports [zookeeper kafka schema-registry]
  {:kafka           kafka
   :zookeeper       zookeeper
   :schema-registry schema-registry})

(defn ->available-ports []
  (->ports (ut/->available-port)
           (ut/->available-port)
           (ut/->available-port)))

(defn ->zookareg-config [ports]
  {:zookareg.schema-registry/schema-registry {:ports      ports
                                              :_kafka     (ig/ref :zookareg.kafka/kafka)
                                              :_zookeeper (ig/ref :zookareg.zookeeper/zookeeper)}
   :zookareg.kafka/kafka                     {:ports      ports
                                              :_zookeeper (ig/ref :zookareg.zookeeper/zookeeper)}
   :zookareg.zookeeper/zookeeper             {:ports ports}})

(defn halt-zookareg! []
  (when @state/system
    (swap! state/system ig/halt!)))

(defn init-zookareg
  ([] (init-zookareg default-ports))
  ([ports] (let [config    (->zookareg-config ports)
                 config-pp (with-out-str (pprint/pprint config))]
             (log/info "starting ZooKaReg with config:" config-pp)
             (try
               (halt-zookareg!)
               (ig/load-namespaces config)
               (reset! state/system (ig/init config))
               (reset! state/config config)
               (catch clojure.lang.ExceptionInfo ex
                 ;; NOTE tear down partially initialised system
                 (ig/halt! (:system (ex-data ex)))
                 (throw (.getCause ex)))))))

(defn with-zookareg
  "Executes f within the context of an embedded zookareg. f will be passed 2 args: config and system"
  [f]
  (try
    (init-zookareg)
    (f @state/config @state/system)
    (finally
      (halt-zookareg!))))

(comment
  ;;;
  (init-zookareg)

  (def ports (-> @zookareg.state/config
                 ut/disqualify-keys
                 :kafka
                 :ports))

  ports
  ;;;
)
