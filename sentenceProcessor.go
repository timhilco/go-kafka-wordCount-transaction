package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var topicIn = "transaction-message-in"
var topicOut = "transaction-message-out"
var topic string

func startProcessingLoop(wg *sync.WaitGroup, bootstrapServer string, workerID string, producer *kafka.Producer, consumer *kafka.Consumer, i int) {
	logger.Info().Msg("-> Starting Processing Loop ....")
	defer wg.Done()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(5000 * time.Millisecond)
	var endTime time.Time
	if i == 0 {
		endTime = time.Now().Add(10 * 60 * time.Second)

	} else {
		x := i * 20
		endTime = time.Now().Add(time.Duration(x) * time.Second)
	}

	run := true
	for run == true {
		select {
		case <-ticker.C:
			t := time.Now()
			fTime := t.Format(time.RFC3339)
			text := fmt.Sprintf("-> Hit Ticker P:%v <> C:%v @ %s - Level: %d", consumer, producer, fTime, i)
			logger.Info().Msg(text)
			logger.Info().Msg(text)
			if t.After(endTime) {
				run = false
				text = fmt.Sprintf("-> Ticker Termination: P:%v <> C:%v @ %s - Level: %d", consumer, producer, fTime, i)
				logger.Info().Msg(text)
			}
		case sig := <-sigchan:
			text := fmt.Sprintf("Caught signal %v: terminating: %s", sig, workerID)
			logger.Info().Msg(text)
			text = fmt.Sprintf("👋 … caught signal and we're done. Slepping 5")
			logger.Info().Msg(text)
			time.Sleep(5 * time.Second)
			run = false
		default:
			// Poll for messages
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				performPipeline(e, consumer, producer)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				text := fmt.Sprintf("default: Ignored case: %v", e)
				logger.Info().Msg(text)
			}

		}
	}
	text := fmt.Sprintf("Closing consumer: %v ", consumer)
	logger.Info().Msg(text)
	consumer.Close()
	text = fmt.Sprintf("Closing producer: %v ", producer)
	logger.Info().Msg(text)
	producer.Close()
}
func performPipeline(e *kafka.Message, consumer *kafka.Consumer, producer *kafka.Producer) {
	ctx := context.Background()
	mPartition := e.TopicPartition.Partition
	mOffset := e.TopicPartition.Offset
	text := fmt.Sprintf("____________ Received <-   %s:%s     Partition: %d [%d] _____________", e.Key, e.Value,
		mPartition, mOffset)
	logger.Debug().Msg(text)
	// Process Message
	sentence := string(e.Value)
	words := strings.Split(sentence, " ")
	wordCount := make(map[string]int)
	for _, w := range words {
		count, ok := wordCount[w]
		if ok {
			wordCount[w] = count + 1
		} else {
			wordCount[w] = 1
		}
	}
	// Start Transaction
	tranactionId := uuid.New().String()
	err := producer.BeginTransaction()
	if err != nil {
		text := fmt.Sprintf("Error: %v", err)
		logger.Info().Msg(text)
	} else {
		text = fmt.Sprintf("-->Begin Transaction:     %s for Producer:  %v ", tranactionId, producer)
		logger.Info().Msg(text)
	}
	for key, value := range wordCount {
		deliveryChan := make(chan kafka.Event)
		topic := topicOut
		kafkaMessage := kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(fmt.Sprintf("%d", value)),
		}
		producer.Produce(&kafkaMessage, deliveryChan)
		pe := <-deliveryChan
		m := pe.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			text := fmt.Sprintf("Delivery failed: %v", m.TopicPartition.Error)
			logger.Info().Msg(text)
		} else {
			text := fmt.Sprintf("Published-> %s:%s - Partition: %d [%d]", m.Key, m.Value,
				m.TopicPartition.Partition, m.TopicPartition.Offset)
			logger.Debug().Msg(text)
		}

		close(deliveryChan)
	}
	partition := e.TopicPartition.Partition
	position := getConsumerPosition(partition, consumer)
	consumerMetadata, err := consumer.GetConsumerGroupMetadata()
	if err != nil {
		logger.Info().Msg(fmt.Sprintf("Failed to get consumer group metadata: %v", err))
	}
	err = producer.SendOffsetsToTransaction(ctx, position, consumerMetadata)
	// Commit or Abort Transaction
	if err != nil {
		logger.Info().Msg("Aborting Transaction: " + tranactionId)
		logger.Info().Msg(fmt.Sprintf(
			"SendOffsetsToTransaction Error: Failed to send offsets to transaction for input partition %v: %s: aborting transaction",
			partition, err))

		err = producer.AbortTransaction(ctx)
		if err != nil {
			text := fmt.Sprintf("Abort Transaction Error: %v", err)
			logger.Info().Msg(text)
		}
		// Rewind this input partition to the last committed offset.
		rewindConsumerPosition(partition, consumer)
	} else {
		text := fmt.Sprintf("SendOffsetsToTransaction - Partition: %d - Position %v", partition, position)
		logger.Info().Msg(text)
		err = producer.CommitTransaction(ctx)
		if err != nil {
			logger.Info().Msg(fmt.Sprintf(
				"CommitTransaction Error: Failed to commit transaction for input partition %v: %s",
				partition, err))

			logger.Info().Msg("Aborting Transaction: " + tranactionId)
			err = producer.AbortTransaction(ctx)
			if err != nil {
				text := fmt.Sprintf("Abort Transaction Error: %v", err)
				logger.Info().Msg(text)
			}

			// Rewind this input partition to the last committed offset.
			rewindConsumerPosition(partition, consumer)
		} else {
			text = fmt.Sprintf("-->Transaction Committed: %s for Producer:  %v -- Partition: %v", tranactionId, producer, partition)
			logger.Info().Msg(text)
		}
	}
}

// This is to be used when the current transaction is aborted.
func rewindConsumerPosition(partition int32, consumer *kafka.Consumer) {
	committed, err := consumer.Committed([]kafka.TopicPartition{{Topic: &topicIn, Partition: partition}}, 10*1000 /* 10s */)
	if err != nil {
		text := fmt.Sprintf("Error: %v", err)
		logger.Info().Msg(text)
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			// No committed offset, reset to earliest
			tp.Offset = kafka.OffsetBeginning
		}

		logger.Info().Msg(fmt.Sprintf("Processor: rewinding input partition %v to offset %v",
			tp.Partition, tp.Offset))

		err = consumer.Seek(tp, -1)
		if err != nil {
			text := fmt.Sprintf("Error: %v", err)
			logger.Info().Msg(text)
		}
	}
}

// getConsumerPosition gets the current position (next offset) for a given input partition.
func getConsumerPosition(partition int32, consumer *kafka.Consumer) []kafka.TopicPartition {
	position, err := consumer.Position([]kafka.TopicPartition{{Topic: &topicIn, Partition: partition}})
	if err != nil {
		text := fmt.Sprintf("Error: %v", err)
		logger.Info().Msg(text)
	}

	return position
}
func groupRebalance(consumer *kafka.Consumer, event kafka.Event) error {

	logger.Info().Msg(fmt.Sprintf("Entering group rebalance -> consumer: %v", consumer.String()))
	topics, _ := consumer.Subscription()
	logger.Info().Msg(fmt.Sprintf("Group Rebalance consumer Topics: %v", topics))
	logger.Info().Msg(fmt.Sprintf("Group Rebalance: group rebalance event: %s", event.String()))
	//logger.Info().Msg(fmt.Sprintf("kbsp-groupRebalance: group rebalance config :%v", config))

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		// Create a producer per input partition.
		logger.Info().Msg(fmt.Sprintf("Group rebalance - AssignPartition Enter"))
		for _, tp := range e.Partitions {
			logger.Info().Msg(fmt.Sprintf("Group rebalance - TopicParition: %v", tp))
		}

		logger.Info().Msg(fmt.Sprintf("Group rebalance - Assign Enter"))
		err := consumer.Assign(e.Partitions)
		if err != nil {
			logger.Info().Msg(fmt.Sprint(err))
		}
		logger.Info().Msg(fmt.Sprintf("Group rebalance - AssignPartition Exit"))

	case kafka.RevokedPartitions:

		logger.Info().Msg(fmt.Sprintf("Group rebalance - RevokedPartitions Enter"))
		for _, tp := range e.Partitions {
			logger.Info().Msg(fmt.Sprintf("Group rebalance - TopicParition: %v", tp))
		}
		err := consumer.Unassign()
		if err != nil {
			logger.Info().Msg(fmt.Sprint(err))
		}
		logger.Info().Msg(fmt.Sprintf("Group rebalance - RevokedPartitions Exit"))
	}

	logger.Info().Msg(fmt.Sprintf("Exiting group rebalance -> consumer: %v", consumer.String()))
	return nil
}
