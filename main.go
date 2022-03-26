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
)

var producer *kafka.Producer
var consumer *kafka.Consumer
var bootstrapServer = "localhost:9092"
var topicIn = "transaction-message-in"
var topicOut = "transaction-message-out"
var topic string

func startProcessingLoop(wg *sync.WaitGroup) {
	fmt.Println("-> Starting Processing Loop ....")
	defer wg.Done()
	consumerTopics := []string{topicIn}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	var err error
	// Creating Consumer
	consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     bootstrapServer,
		"broker.address.family": "v4",
		"group.id":              "word-group-1",
		"session.timeout.ms":    6000,
		"isolation.level":       "read_committed",
		"enable.auto.commit":    "false"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Consumer %v\n", consumer)
	err = consumer.SubscribeTopics(consumerTopics, groupRebalance)
	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			// Poll for messages
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				mPartition := e.TopicPartition.Partition
				mOffset := e.TopicPartition.Offset
				fmt.Printf("____________ Received <-        Partition: %d [%d] _____________\n",
					mPartition, mOffset)
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
				producer.BeginTransaction()
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
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					} else {
						fmt.Printf("Published -> Partition: %d [%d]\n",
							m.TopicPartition.Partition, m.TopicPartition.Offset)
					}

					close(deliveryChan)
				}
				partition := e.TopicPartition.Partition
				position := getConsumerPosition(partition)
				consumerMetadata, err := consumer.GetConsumerGroupMetadata()
				if err != nil {
					fmt.Println(fmt.Sprintf("Failed to get consumer group metadata: %v", err))
				}
				err = producer.SendOffsetsToTransaction(nil, position, consumerMetadata)
				// Commit or Abort Transaction
				if err != nil {
					fmt.Println(fmt.Sprintf(
						"Processor: Failed to send offsets to transaction for input partition %v: %s: aborting transaction",
						partition, err))

					err = producer.AbortTransaction(nil)
					if err != nil {
						fmt.Println(err)
					}
					// Rewind this input partition to the last committed offset.
					rewindConsumerPosition(partition)
				} else {
					err = producer.CommitTransaction(nil)
					if err != nil {
						fmt.Println(fmt.Sprintf(
							"Processor: Failed to commit transaction for input partition %v: %s",
							partition, err))

						err = producer.AbortTransaction(nil)
						if err != nil {
							fmt.Println(err)
						}

						// Rewind this input partition to the last committed offset.
						rewindConsumerPosition(partition)
					}
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("default: Ignored case: %v\n", e)
			}

		}
	}
	fmt.Printf("Closing consumer\n")
	consumer.Close()
}

// This is to be used when the current transaction is aborted.
func rewindConsumerPosition(partition int32) {
	committed, err := consumer.Committed([]kafka.TopicPartition{{Topic: &topicIn, Partition: partition}}, 10*1000 /* 10s */)
	if err != nil {
		fmt.Println(err)
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			// No committed offset, reset to earliest
			tp.Offset = kafka.OffsetBeginning
		}

		fmt.Println(fmt.Sprintf("Processor: rewinding input partition %v to offset %v",
			tp.Partition, tp.Offset))

		err = consumer.Seek(tp, -1)
		if err != nil {
			fmt.Println(err)
		}
	}
}

// getConsumerPosition gets the current position (next offset) for a given input partition.
func getConsumerPosition(partition int32) []kafka.TopicPartition {
	position, err := consumer.Position([]kafka.TopicPartition{{Topic: &topicIn, Partition: partition}})
	if err != nil {
		fmt.Println(err)
	}

	return position
}
func groupRebalance(consumer *kafka.Consumer, event kafka.Event) error {

	fmt.Println(fmt.Sprintf("Entering group rebalance -> consumer: %v", consumer.String()))
	topics, _ := consumer.Subscription()
	fmt.Println(fmt.Sprintf("Group rebalance consumer Topics: %v", topics))
	//fmt.Println(fmt.Sprintf("kbsp-groupRebalance: group rebalance event: %s", event.String()))
	//fmt.Println(fmt.Sprintf("kbsp-groupRebalance: group rebalance config :%v", config))

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		// Create a producer per input partition.
		fmt.Println(fmt.Sprintf("Group rebalance - AssignPartition Enter"))
		for _, tp := range e.Partitions {
			fmt.Println(fmt.Sprintf("Group rebalance - TopicParition: %v", tp))
		}

		fmt.Println(fmt.Sprintf("Group rebalance - Assign"))
		err := consumer.Assign(e.Partitions)
		if err != nil {
			fmt.Println(fmt.Sprint(err))
		}
		fmt.Println(fmt.Sprintf("Group rebalance - AssignPartition Exit"))

	case kafka.RevokedPartitions:

		fmt.Println(fmt.Sprintf("Group rebalance - RevokedPartitions Enter"))
		for _, tp := range e.Partitions {
			fmt.Println(fmt.Sprintf("Group rebalance - TopicParition: %v", tp))
		}
		err := consumer.Unassign()
		if err != nil {
			fmt.Println(fmt.Sprint(err))
		}
		fmt.Println(fmt.Sprintf("Group rebalance - RevokedPartitions Exit"))
	}
	//fmt.Println(fmt.Sprintf("kbsp-groupRebalance: group rebalance - Exit --------------------"))

	return nil
}
func main() {
	var tranID = "prod-1"
	if len(os.Args) > 1 {
		tranID = os.Args[1]
	}
	var wg sync.WaitGroup
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServer,
		"enable.idempotence": true,
		"transactional.id":   tranID,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", producer)
	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.InitTransactions(ctx)
	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
	wg.Add(1)
	go startProcessingLoop(&wg)
	wg.Wait()

}
