package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func NewMultiWithFile(isDebug bool) zerolog.Logger {
	logLevel := zerolog.InfoLevel
	if isDebug {
		logLevel = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}
	//file, err := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	file, err := os.OpenFile("logs.txt", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logger.Info().Msg("Error: HilcoLogger file error")
	}
	//multi := zerolog.MultiLevelWriter(consoleWriter, os.Stdout, file)
	multi := zerolog.MultiLevelWriter(consoleWriter, file)
	logger := zerolog.New(multi).With().Timestamp().Logger()
	return logger
}
func main() {
	logger = NewMultiWithFile(false)

	var tranID = "prod-"

	var wg sync.WaitGroup
	bootstrapServer := "localhost:9092"
	partitions := 10
	manageKafkaTopics(bootstrapServer, partitions)
	topic := "transaction-message-in"
	wg.Add(1)
	go publishSentences(&wg, bootstrapServer, topic, 50)
	for i := 0; i < 5; i++ {
		label := strconv.Itoa(i)
		id := tranID + label
		//id := tranID + "1"
		logger.Info().Msg("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		logger.Info().Msg("Starting worker: " + label)
		logger.Info().Msg("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		wg.Add(1)
		go worker(&wg, bootstrapServer, id, i)
		time.Sleep(5 * time.Second)

	}
	wg.Wait()

}
func worker(wg *sync.WaitGroup, bootstrapServer string, tranID string, i int) {

	var err error
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServer,
		"enable.idempotence": true,
		"transactional.id":   tranID,
	})
	if err != nil {
		text := fmt.Sprintf("Failed to create producer: %s", err)
		logger.Info().Msg(text)
		os.Exit(1)
	}

	text := fmt.Sprintf("Created Producer %v", producer)
	logger.Info().Msg(text)

	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		text := fmt.Sprintf("Error: %s", err)
		logger.Info().Msg(text)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.InitTransactions(ctx)
	if err != nil {
		text := fmt.Sprintf("Error: %s", err)
		logger.Info().Msg(text)
		os.Exit(1)
	} else {
		text := fmt.Sprintf("InitTransaction successful  for %v", producer)
		logger.Info().Msg(text)
	}

	var consumer *kafka.Consumer
	consumerTopics := []string{topicIn}

	// Creating Consumer
	consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     bootstrapServer,
		"broker.address.family": "v4",
		"group.id":              "word-master-group",
		"session.timeout.ms":    6000,
		"isolation.level":       "read_committed",
		"enable.auto.commit":    "false"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s", err)
		os.Exit(1)
	}
	text = fmt.Sprintf("Created Consumer %v", consumer)
	logger.Info().Msg(text)
	err = consumer.SubscribeTopics(consumerTopics, groupRebalance)
	startProcessingLoop(wg, bootstrapServer, tranID, producer, consumer, i)

}
func manageKafkaTopics(bootstrapServer string, partitions int) {
	logger.Info().Msg("-----------------------------------")
	logger.Info().Msg("Managing topics in Broker")
	logger.Info().Msg("-----------------------------------")
	// Create AdminClient instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewAdminClient

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer}

	// Variable p holds the new AdminClient instance.
	a, e := kafka.NewAdminClient(&cm)
	// Make sure we close it when we're done
	defer a.Close()

	// Check for errors in creating the AdminClient
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				text := fmt.Sprintf("😢 Can't create the AdminClient because you've configured it wrong (code: %d)!\n\t%vTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
				logger.Info().Msg(text)
			default:
				text := fmt.Sprintf("😢 Can't create the AdminClient (Kafka error code %d)\tError: %v", ec, e)
				logger.Info().Msg(text)
			}
		} else {
			// It's not a kafka.Error
			text := fmt.Sprintf("😢 Oh noes, there's a generic error creating the AdminClient! %v", e.Error())
			logger.Info().Msg(text)
		}

	} else {

		logger.Info().Msg("✔️ Created AdminClient")

		// Create a context for use when calling some of these functions
		// This lets you set a variable timeout on invoking these calls
		// If the timeout passes then an error is returned.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// Get the ClusterID
		if c, e := a.ClusterID(ctx); e != nil {
			text := fmt.Sprintf("😢 Error getting ClusterID\tError: %v", e)
			logger.Info().Msg(text)
		} else {
			text := fmt.Sprintf("✔️ ClusterID: %v", c)
			logger.Info().Msg(text)
		}

		// Start the context timer again (otherwise it carries on from the original deadline)
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// Get the ControllerID
		if c, e := a.ControllerID(ctx); e != nil {
			text := fmt.Sprintf("😢 Error getting ControllerID\tError: %v", e)
			logger.Info().Msg(text)
		} else {
			text := fmt.Sprintf("✔️ ControllerID: %v", c)
			logger.Info().Msg(text)
		}

		// Get some metadata
		if md, e := a.GetMetadata(nil, false, int(5*time.Second)); e != nil {
			text := fmt.Sprintf("😢 Error getting cluster Metadata\tError: %v", e)
			logger.Info().Msg(text)
		} else {
			// Print the originating broker info
			text := fmt.Sprintf("✔️ Metadata [Originating broker]")
			logger.Info().Msg(text)
			b := md.OriginatingBroker
			text = fmt.Sprintf("\t[ID %d] %v", b.ID, b.Host)
			logger.Info().Msg(text)

			// Print the brokers
			text = fmt.Sprintf("✔️ Metadata [brokers]")
			logger.Info().Msg(text)
			for _, b := range md.Brokers {
				text := fmt.Sprintf("\t[ID %d] %v:%d", b.ID, b.Host, b.Port)
				logger.Info().Msg(text)
			}
			// Print the topics
			text = fmt.Sprintf("✔️ Metadata [topics]")
			logger.Info().Msg(text)
			for _, t := range md.Topics {
				text = fmt.Sprintf("\t(%v partitions)\t%v", len(t.Partitions), t.Topic)
				logger.Info().Msg(text)
			}

		}
		// Delete the following topics
		logger.Info().Msg("-----------------------------------")
		logger.Info().Msg("Deleting topics in Broker Starting")
		logger.Info().Msg("-----------------------------------")
		deleteTopics := []string{
			"transaction-message-in",
			"transaction-message-out",
		}
		do := kafka.SetAdminOperationTimeout(20 * time.Second)
		result, err := a.DeleteTopics(ctx, deleteTopics, do)
		if err != nil {
			text := fmt.Sprintf("Error: %v", err)
			logger.Info().Msg(text)
		}
		text := fmt.Sprintf("👋 … and we're done. Slepping 5")
		logger.Info().Msg(text)
		time.Sleep(5 * time.Second)
		text = fmt.Sprintf("%v", result)
		logger.Info().Msg(text)
		logger.Info().Msg("-----------------------------------")
		logger.Info().Msg("Deleting topics in Broker completed")
		logger.Info().Msg("-----------------------------------")

		//Adding the following topics
		logger.Info().Msg("-----------------------------------")
		logger.Info().Msg("Adding topics in Broker starting")
		logger.Info().Msg("-----------------------------------")
		addTopics := []kafka.TopicSpecification{
			{
				Topic:             "transaction-message-in",
				NumPartitions:     partitions,
				ReplicationFactor: 1,
			},
			{
				Topic:             "transaction-message-out",
				NumPartitions:     partitions,
				ReplicationFactor: 1,
			},
		}

		result, err = a.CreateTopics(ctx, addTopics, do)
		if err != nil {
			text := fmt.Sprintf("Error: %v", err)
			logger.Info().Msg(text)
		}
		text = fmt.Sprintf("👋 … and we're done. Slepping 5")
		logger.Info().Msg(text)
		time.Sleep(5 * time.Second)
		text = fmt.Sprintf("%v", result)
		logger.Info().Msg(text)
		logger.Info().Msg("-----------------------------------")
		logger.Info().Msg("Adding topics in Broker completed")
		logger.Info().Msg("-----------------------------------")

	}

}
