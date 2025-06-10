(ns flinkfintechpoc.connectors
  (:require [babashka.http-client :as http]
            [charred.api :as json]))

(defn register-postgres-customers-connector!
  [{:keys [connector-name
           connect-url
           database-host
           database-port
           database-user
           database-password
           database-name
           topic-prefix]
    :or {connect-url "http://localhost:8083"
         database-host "postgres"
         database-port 5432
         database-user "postgres"
         database-password "postgres"
         database-name "outbox_demo"
         topic-prefix "customers"}}]
  (let [connector-config {:name connector-name
                          :config {"connector.class" "io.debezium.connector.postgresql.PostgresConnector"
                                   "tasks.max" "1"
                                   "database.hostname" database-host
                                   "database.port" (str database-port)
                                   "database.user" database-user
                                   "database.password" database-password
                                   "database.dbname" database-name
                                   "topic.prefix" topic-prefix
                                   "schema.include.list" "public"
                                   "table.include.list" "public.customers"
                                   "plugin.name" "pgoutput"
                                   "key.converter" "org.apache.kafka.connect.json.JsonConverter",
                                   "key.converter.schemas.enable" "false",
                                   "value.converter" "org.apache.kafka.connect.json.JsonConverter",
                                   "value.converter.schemas.enable" "false"}}
        response (http/post (str connect-url "/connectors")
                            {:headers {"Content-Type" "application/json"
                                       "Accept" "application/json"}
                             :body (json/write-json-str connector-config)})]
    (clojure.pprint/pprint response)
    response))

(defn register-postgres-outbox-connector!
  [{:keys [connector-name
           connect-url
           database-host
           database-port
           database-user
           database-password
           database-name
           topic-prefix]
    :or {connect-url "http://localhost:8083"
         database-host "postgres"
         database-port 5432
         database-user "postgres"
         database-password "postgres"
         database-name "outbox_demo"
         topic-prefix "outbox"}}]
  (let [connector-config {:name connector-name
                          :config {"connector.class" "io.debezium.connector.postgresql.PostgresConnector"
                                   "tasks.max" "1"
                                   "database.hostname" database-host
                                   "database.port" (str database-port)
                                   "database.user" database-user
                                   "database.password" database-password
                                   "database.dbname" database-name
                                   "topic.prefix" topic-prefix
                                   "schema.include.list" "public"
                                   "table.include.list" "public.outbox"
                                   "plugin.name" "pgoutput"
                                   "transforms" "outbox"
                                   "transforms.outbox.type" "io.debezium.transforms.outbox.EventRouter"
                                   "key.converter" "io.confluent.connect.avro.AvroConverter"
                                   "value.converter" "io.confluent.connect.avro.AvroConverter"
                                   "key.converter.schema.registry.url" "http://schema-registry:8081"
                                   "value.converter.schema.registry.url" "http://schema-registry:8081"
                                   "transforms.outbox.table.expand.json.payload" "true"}}
        response (http/post (str connect-url "/connectors")
                            {:headers {"Content-Type" "application/json"
                                       "Accept" "application/json"}
                             :body (json/write-json-str connector-config)})]
    (clojure.pprint/pprint response)
    response))

(defn list-connectors
  [project-id service-name]
  (let [aiven-token "***"
        url (str "https://api.aiven.io/v1/project/" project-id "/service/" service-name "/connectors")
        response (http/get url {:headers {"Accept" "application/json"
                                          "Authorization" (str "aivenv1 " aiven-token)}})]
    (->> (json/read-json (:body response) :key-fn keyword)
         :connectors
         (filter (fn [connector]
                   (= "io.debezium.connector.postgresql.PostgresConnector"
                      (-> connector :plugin :class))))
         (filter (fn [connector]
                   #_(some? (-> connector :config :database.server.name))
                   (nil? (-> connector :config :topic.prefix))))
         (map :name))))

(comment
  (list-connectors "kroo-development" "kroo-development-kafka-connect"))
