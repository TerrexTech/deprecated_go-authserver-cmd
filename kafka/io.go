package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
)

// IO provides channels for interacting with Kafka.
// Note: All receive-channels must be read from to prevent deadlock.
type ConsumerIO struct {
	consumerErrChan    <-chan error
	consumerMsgChan    <-chan *sarama.ConsumerMessage
	consumerOffsetChan chan<- *sarama.ConsumerMessage
}

// Errors returns send-channel where consumer errors are published.
func (cio *ConsumerIO) Errors() <-chan error {
	return cio.consumerErrChan
}

// Messages returns send-channel where consumer messages are published.
func (cio *ConsumerIO) Messages() <-chan *sarama.ConsumerMessage {
	return cio.consumerMsgChan
}

// MarkOffset marks the consumer message-offset to be committed.
// This should be used once a message has done its job.
func (cio *ConsumerIO) MarkOffset() chan<- *sarama.ConsumerMessage {
	return cio.consumerOffsetChan
}

type ProducerIO struct {
	id              string
	producerErrChan <-chan *sarama.ProducerError
	inputChan       chan<- *model.KafkaResponse
}

func (pio *ProducerIO) ID() string {
	return pio.id
}

// Errors returns send-channel where producer errors are published.
func (pio *ProducerIO) Errors() <-chan *sarama.ProducerError {
	return pio.producerErrChan
}

// Input returns receive-channel where kafka-responses can be produced.
func (pio *ProducerIO) Input() chan<- *model.KafkaResponse {
	return pio.inputChan
}

type ProducerESQueryIO struct {
	id              string
	producerErrChan <-chan *sarama.ProducerError
	inputChan       chan<- *model.EventStoreQuery
}

func (pio *ProducerESQueryIO) ID() string {
	return pio.id
}

// Errors returns send-channel where producer errors are published.
func (pio *ProducerESQueryIO) Errors() <-chan *sarama.ProducerError {
	return pio.producerErrChan
}

// Input returns receive-channel where kafka-responses can be produced.
func (pio *ProducerESQueryIO) Input() chan<- *model.EventStoreQuery {
	return pio.inputChan
}
