package main

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-authserver-cmd/auth"
	ec "github.com/TerrexTech/go-authserver-cmd/errors"
	"github.com/TerrexTech/go-authserver-cmd/kafka"
	model "github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

type kioConfig struct {
	ErrChan chan error
	DB      auth.DBI
	KA      *kafka.Adapter
	Topic   string
}

// Creates a KafkaIO from KafkaAdapter based on set environment variables.
func initEventsConsumer(
	kio kioConfig,
	eventsRespChan chan<- *model.EventStoreQuery,
	regRespChan chan<- *model.KafkaResponse,
) {
	eventsCons, err := kio.KA.EnsureConsumerIO(kio.Topic, kio.Topic, false)
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Event-Consumer")
		kio.ErrChan <- err
	}

	for msg := range eventsCons.Messages() {
		go handleEvents(kio.DB, msg, eventsRespChan, regRespChan, eventsCons.MarkOffset())
	}
}

func handleEvents(
	db auth.DBI,
	msg *sarama.ConsumerMessage,
	respChan chan<- *model.EventStoreQuery,
	regRespChan chan<- *model.KafkaResponse,
	offsetMarkChan chan<- *sarama.ConsumerMessage,
) {
	event := &model.Event{}
	err := json.Unmarshal(msg.Value, event)
	if err != nil {
		err = errors.Wrap(err, "EventsConsumer: Error Unmarshalling Event")
		log.Println(err)
		return
	}

	if event.AggregateID == 1 {
		offsetMarkChan <- msg
		log.Println("==========+++++++++++++++++++")
		currVersion, err := db.GetMaxVersion()
		if err != nil {
			err = errors.Wrap(err, "Error fetching max version")
			log.Println(err)
			regRespChan <- &model.KafkaResponse{
				AggregateID:   1,
				CorrelationID: event.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     ec.InternalError,
			}
			return
		}

		esQuery := &model.EventStoreQuery{
			AggregateID:      1,
			CorrelationID:    event.CorrelationID,
			AggregateVersion: currVersion,
			YearBucket:       2018,
		}
		log.Println("===================")
		log.Printf("%+v", esQuery)
		respChan <- esQuery
	}
}

func initESQueryProducer(kio kioConfig, regRespChan chan<- *model.KafkaResponse) {
	esQueryCons, err := kio.KA.EnsureConsumerIO(
		kio.Topic, kio.Topic, false,
	)
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka EventsQuery-response Consumer")
		kio.ErrChan <- err
	}

	for msg := range esQueryCons.Messages() {
		go handleESQueryResponse(
			kio.DB,
			regRespChan,
			msg,
			esQueryCons.MarkOffset(),
		)
	}
}

func handleESQueryResponse(
	db auth.DBI,
	regRespChan chan<- *model.KafkaResponse,
	msg *sarama.ConsumerMessage,
	offsetMarkChan chan<- *sarama.ConsumerMessage,
) {
	// TODO: Find appropriate handling fir marking message-offsets
	kr := &model.KafkaResponse{}
	err := json.Unmarshal(msg.Value, kr)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling message into KafkaResponse")
		log.Println(err)
		return
	}

	if kr.AggregateID == 1 {
		offsetMarkChan <- msg

		events := &[]model.Event{}
		err = json.Unmarshal([]byte(kr.Result), events)
		if err != nil {
			err = errors.Wrap(err, "Error unmarshalling KafkaResult into Register-Events")
			regRespChan <- &model.KafkaResponse{
				AggregateID:   1,
				CorrelationID: kr.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     ec.InternalError,
			}
			return
		}

		for _, event := range *events {
			log.Println("\n\n\n\n\n\n=======>>>>>>>>>>>>>")
			log.Printf("+%v", event)

			user := &auth.User{}
			err := user.UnmarshalJSON([]byte(event.Data))
			if err != nil {
				err = errors.Wrap(err, "Error Unmarsahlling EventStoreQuery into User Aggregate")
				log.Println(err)
				regRespChan <- &model.KafkaResponse{
					AggregateID:   1,
					CorrelationID: event.CorrelationID,
					Error:         err.Error(),
					ErrorCode:     ec.InternalError,
				}
				continue
			}
			user.Version = event.Version
			authUser, err, errCode := db.Register(user)
			if err != nil {
				err = errors.Wrap(err, "Error inserting user into Database")
				log.Println(err)
				regRespChan <- &model.KafkaResponse{
					AggregateID:   1,
					CorrelationID: event.CorrelationID,
					Error:         err.Error(),
					ErrorCode:     errCode,
				}
				continue
			}
			authUserBytes, err := json.Marshal(authUser)
			if err != nil {
				err = errors.Wrapf(err, "Error Marshalling User from Event ID: %s", event.UUID)
				regRespChan <- &model.KafkaResponse{
					AggregateID:   1,
					CorrelationID: event.CorrelationID,
					Error:         err.Error(),
					ErrorCode:     ec.InternalError,
				}
				continue
			}

			log.Println("\n\n" + event.CorrelationID.String())
			log.Println(string(event.Data))
			regRespChan <- &model.KafkaResponse{
				AggregateID:   1,
				CorrelationID: event.CorrelationID,
				Result:        authUserBytes,
			}
		}
	}
}
