(ns flinkfintechpoc.app
  (:gen-class)
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [charred.api :as json]
            [faker.name :as faker-name]
            [faker.lorem :as faker-lorem]
            [faker.internet :as faker-internet]
            [flinkfintechpoc.connectors :as connectors])
  (:import (java.math RoundingMode)
           (java.time Instant)
           (java.util Random)))

(defn random-bigdecimal
  [min max decimal-places]
  (let [random (Random.)
        range (.subtract max min)
        random-factor (BigDecimal. (.nextDouble random))
        result (.add min (.multiply range random-factor))]
    (.setScale result decimal-places RoundingMode/HALF_UP)))

(def transaction-types [:card :outbound :inbound :internal])
(def currencies ["USD" "EUR" "GBP"])

(defn random-account-number
  []
  (str "ACC-" (rand-int 1000)))

(defn random-small-amount
  []
  (random-bigdecimal
    (BigDecimal. "0.1")
    (BigDecimal. "10")
    2))

(defn gen-transaction []
  (let [type (rand-nth transaction-types)
        amount (random-small-amount)
        currency (rand-nth currencies)
        from-account (random-account-number)
        to-account (random-account-number)
        description (faker-lorem/paragraphs 1)]
    {:transaction_type (name type)
     :amount (bigdec amount)
     :currency currency
     :from_account (when (not= type :inbound) from-account)
     :to_account (when (not= type :outbound) to-account)
     :transaction_date (Instant/now)
     :description description
     :created_at (Instant/now)}))

(def CUSTOMER_AGGREGATE_TYPE "Customer")
(def CUSTOMER_UPDATED_EVENT_TYPE "CustomerUpdated")
(def CUSTOMER_CREATED_EVENT_TYPE "CustomerCreated")

(def db-spec {:dbtype "postgresql"
              :dbname "outbox_demo"
              :host "localhost"
              :port 5432
              :user "postgres"
              :password "postgres"})

(defn init-db! []
      (with-open [conn (jdbc/get-connection db-spec)]
                 (jdbc/execute! conn
                                ["CREATE TABLE IF NOT EXISTS customers (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        email VARCHAR(255) NOT NULL UNIQUE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                      )"])
                 (jdbc/execute! conn
                                ["CREATE TABLE IF NOT EXISTS outbox (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        aggregatetype VARCHAR(255) NOT NULL,
                        aggregateid VARCHAR(255) NOT NULL,
                        type VARCHAR(255) NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                      )"])))

(defn write-outbox!
      [conn event]
      (jdbc/execute-one!
        conn
        #_["SELECT pg_logical_emit_message(true, 'outbox', ?)"
           (json/write-json-str event)]
        ["INSERT INTO outbox (aggregatetype, aggregateid, type, payload) VALUES (?, ?, ?, ?::jsonb)"
         (:aggregatetype event)
         (:aggregateid event)
         (:type event)
         (json/write-json-str (:payload event))]))

(defn update-customer-email!
      [{:keys [customer-id email]}]
      (with-open [conn (jdbc/get-connection db-spec)]
                 (jdbc/with-transaction
                   [tx conn]
                   (let [customer (jdbc/execute-one!
                                    tx
                                    ["UPDATE customers SET email = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? RETURNING *"
                                     email customer-id]
                                    {:return-keys true
                                     :builder-fn rs/as-unqualified-maps})
                         event {:type CUSTOMER_UPDATED_EVENT_TYPE
                                :aggregatetype CUSTOMER_AGGREGATE_TYPE
                                :aggregateid customer-id
                                :payload {:id customer-id
                                          :email email
                                          :updated_at (str (:updated_at customer))}}]
                        (write-outbox! tx event)
                        #_(jdbc/execute-one!
                            tx
                            #_["SELECT pg_logical_emit_message(true, 'outbox', ?)"
                               (json/write-json-str event)]
                            ["INSERT INTO outbox (aggregatetype, aggregateid, type, payload) VALUES (?, ?, ?, ?::jsonb)"
                             (:aggregatetype event)
                             (:aggregateid event)
                             (:type event)
                             (json/write-json-str (:payload event))])
                        customer))))

(defn create-customer!
      [{:keys [name email]}]
      (with-open [conn (jdbc/get-connection db-spec)]
                 (jdbc/with-transaction
                   [tx conn]
                   (let [customer (jdbc/execute-one!
                                    tx
                                    ["INSERT INTO customers (name, email) VALUES (?, ?) RETURNING *"
                                     name email]
                                    {:return-keys true
                                     :builder-fn rs/as-unqualified-maps})
                         customer-id (:id customer)
                         event {:type CUSTOMER_CREATED_EVENT_TYPE
                                :aggregatetype CUSTOMER_AGGREGATE_TYPE
                                :aggregateid customer-id
                                :payload {:id customer-id
                                          :name name
                                          :email email
                                          :created_at (str (:created_at customer))}}]
                        (write-outbox! tx event)
                        #_(jdbc/execute-one!
                            tx
                            ["INSERT INTO outbox (aggregatetype, aggregateid, type, payload) VALUES (?, ?, ?, ?::jsonb)"
                             (:aggregatetype event)
                             (:aggregateid event)
                             (:type event)
                             (json/write-json-str (:payload event))]
                            #_["SELECT pg_logical_emit_message(true, 'outbox', ?)"
                               (json/write-json-str event)])

                        customer))))

(defn -main
      []
      (init-db!)
      (let [customer (create-customer! {:name (first (faker-name/names))
                                        :email (faker-internet/email)})
            new-email (faker-internet/email)]
           (println "Random customer created:")
           (clojure.pprint/pprint customer)
           (update-customer-email!
             {:customer-id (:id customer)
              :email new-email})))

(comment
  ;; Example usage of register-postgres-outbox-connector!

  ;; Register a connector with default settings
  (connectors/register-postgres-outbox-connector!
    {:connector-name "postgres-outbox-connector"})

  ;; Example usage of register-postgres-customers-connector!

  ;; Register a customers connector with default settings
  (connectors/register-postgres-customers-connector!
    {:connector-name "postgres-customers-connector"})

  ;; Register a connector with custom settings
  (connectors/register-postgres-outbox-connector!
    {:connector-name "custom-outbox-connector"
     :connect-url "http://localhost:8083"
     :database-host "postgres"
     :database-port 5432
     :database-user "postgres"
     :database-password "postgres"
     :database-name "outbox_demo"
     :topic-prefix "custom-outbox"})

  ;; Register a customers connector with custom settings
  (connectors/register-postgres-customers-connector!
    {:connector-name "custom-customers-connector"
     :connect-url "http://localhost:8083"
     :database-host "postgres"
     :database-port 5432
     :database-user "postgres"
     :database-password "postgres"
     :database-name "outbox_demo"
     :topic-prefix "custom-customers"})

  ;; For Aiven Kafka Connect service
  (connectors/register-postgres-outbox-connector!
    {:connector-name "aiven-outbox-connector"
     :connect-url "https://api.aiven.io/v1/project/my-project/service/my-kafka-connect/connectors"
     :database-host "my-postgres-host"
     :database-port 5432
     :database-user "my-db-user"
     :database-password "my-db-password"
     :database-name "my-database"
     :topic-prefix "aiven-outbox"}))
