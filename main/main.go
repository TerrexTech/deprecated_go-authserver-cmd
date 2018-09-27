package main

import (
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-authserver-cmd/auth"
	"github.com/TerrexTech/go-authserver-cmd/kafka"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func main() {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",

		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_TOPIC_REGISTER",

		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_COLLECTION",
		"MONGO_TIMEOUT",
	)
	if err != nil {
		log.Fatalf(
			"Error: Environment variable %s is required but was not found", missingVar,
		)
	}

	// Mongo Setup
	hosts := os.Getenv("MONGO_HOSTS")
	username := os.Getenv("MONGO_USERNAME")
	password := os.Getenv("MONGO_PASSWORD")
	database := os.Getenv("MONGO_DATABASE")
	collection := os.Getenv("MONGO_COLLECTION")

	timeoutMilliStr := os.Getenv("MONGO_TIMEOUT")
	parsedTimeoutMilli, err := strconv.Atoi(timeoutMilliStr)
	if err != nil {
		err = errors.Wrap(err, "Error converting Timeout value to int32")
		log.Println(err)
		log.Println("MONGO_TIMEOUT value will be set to 3000 as default value")
		parsedTimeoutMilli = 3000
	}
	timeoutMilli := uint32(parsedTimeoutMilli)

	config := auth.DBIConfig{
		Hosts:               *commonutil.ParseHosts(hosts),
		Username:            username,
		Password:            password,
		TimeoutMilliseconds: timeoutMilli,
		Database:            database,
		Collection:          collection,
	}
	db, err := auth.EnsureAuthDB(config)
	if err != nil {
		err = errors.Wrap(err, "Error connecting to AuthDB")
		log.Println(err)
		return
	}

	// Kafka Setup
	brokers := os.Getenv("KAFKA_BROKERS")
	kafkaAdapter := &kafka.Adapter{
		Brokers: *commonutil.ParseHosts(brokers),
	}

	errChan := make(chan error)

	// EventStoreQuery Producer
	esQueryProdTopic := os.Getenv("KAFKA_PRODUCER_EVENT_QUERY_TOPIC")
	esQueryProd, err := kafkaAdapter.EnsureProducerESQueryIO(esQueryProdTopic, esQueryProdTopic, false)
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka EventsQuery-request Producer")
		log.Fatalln(err)
	}

	// Registration Response Producer
	regRespTopic := os.Getenv("KAFKA_PRODUCER_TOPIC_REGISTER")
	regRespProd, err := kafkaAdapter.EnsureProducerIO(regRespTopic, false)
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Registration-response Producer")
		log.Fatalln(err)
	}

	eventsConsTopic := os.Getenv("KAFKA_CONSUMER_EVENT_TOPIC")
	kc := kioConfig{
		ErrChan: errChan,
		DB:      db,
		KA:      kafkaAdapter,
		Topic:   eventsConsTopic,
	}
	go initEventsConsumer(kc, esQueryProd.Input(), regRespProd.Input())

	// EventsQuery-Result Consumer
	esQueryConsTopic := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_TOPIC")
	kc = kioConfig{
		ErrChan: errChan,
		DB:      db,
		KA:      kafkaAdapter,
		Topic:   esQueryConsTopic,
	}
	go initESQueryProducer(kc, regRespProd.Input())

	for err = range errChan {
		err = errors.Wrap(err, "Fatal Error")
		log.Fatalln(err)
	}
}
