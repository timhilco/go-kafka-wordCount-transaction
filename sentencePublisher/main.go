package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaProcessor "github.com/timhilco/go-abpcworkflow/kafka"
)

//var program kafkaProgram.KafkaBasedSchedulerProcessor
var publisher kafkaProcessor.KafkaMessagePublisher

const bootstrapServer string = "localhost:9092"
const topicOut = "transaction-message-in"

func readFile() []string {
	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(path)
	var fileName = path + "/file1.txt"
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error: %s", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lines []string

	for scanner.Scan() {
		text := scanner.Text()
		lines = append(lines, text)
	}
	// if err := scanner.Err (); err! = nil {
	// 	fmt.Printf ("Error: %s", err)
	// }
	return lines
}
func main() {
	//deleteKafkaTopics()
	var err error
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		// 	"enable.idempotence": true,
		// 	"transactional.id":   "prod-1",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	topic := topicOut
	// sentences1 := []string{
	// 	"This is the first sentence",
	// 	"This is the second sentence",
	// }
	sentences := readFile()
	for _, s := range sentences {
		deliveryChan := make(chan kafka.Event)
		key := len(s)
		keyA := strconv.Itoa(key)
		kafkaMessage := kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(keyA),
			Value:          []byte(s),
		}
		err := producer.Produce(&kafkaMessage, deliveryChan)
		if err != nil {
			fmt.Printf("Error: %s", err)
		}
		pe := <-deliveryChan
		m := pe.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}

		close(deliveryChan)
	}
}

func deleteKafkaTopics() {
	fmt.Println("-----------------------------------")
	fmt.Println("Deleting topics in Broker")
	fmt.Println("-----------------------------------")
	// Create AdminClient instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewAdminClient

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092"}

	// Variable p holds the new AdminClient instance.
	a, e := kafka.NewAdminClient(&cm)
	// Make sure we close it when we're done
	defer a.Close()

	// Check for errors in creating the AdminClient
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				fmt.Printf("😢 Can't create the AdminClient because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
			default:
				fmt.Printf("😢 Can't create the AdminClient (Kafka error code %d)\n\tError: %v\n", ec, e)
			}
		} else {
			// It's not a kafka.Error
			fmt.Printf("😢 Oh noes, there's a generic error creating the AdminClient! %v", e.Error())
		}

	} else {

		fmt.Println("✔️ Created AdminClient")

		// Create a context for use when calling some of these functions
		// This lets you set a variable timeout on invoking these calls
		// If the timeout passes then an error is returned.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get the ClusterID
		if c, e := a.ClusterID(ctx); e != nil {
			fmt.Printf("😢 Error getting ClusterID\n\tError: %v\n", e)
		} else {
			fmt.Printf("✔️ ClusterID: %v\n", c)
		}

		// Start the context timer again (otherwise it carries on from the original deadline)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

		// Get the ControllerID
		if c, e := a.ControllerID(ctx); e != nil {
			fmt.Printf("😢 Error getting ControllerID\n\tError: %v\n", e)
		} else {
			fmt.Printf("✔️ ControllerID: %v\n", c)
		}

		// Get some metadata
		if md, e := a.GetMetadata(nil, false, int(5*time.Second)); e != nil {
			fmt.Printf("😢 Error getting cluster Metadata\n\tError: %v\n", e)
		} else {
			// Print the originating broker info
			fmt.Printf("✔️ Metadata [Originating broker]\n")
			b := md.OriginatingBroker
			fmt.Printf("\t[ID %d] %v\n", b.ID, b.Host)

			// Print the brokers
			fmt.Printf("✔️ Metadata [brokers]\n")
			for _, b := range md.Brokers {
				fmt.Printf("\t[ID %d] %v:%d\n", b.ID, b.Host, b.Port)
			}
			// Print the topics
			fmt.Printf("✔️ Metadata [topics]\n")
			for _, t := range md.Topics {
				fmt.Printf("\t(%v partitions)\t%v\n", len(t.Partitions), t.Topic)
			}

		}
		// Delete the following topics
		deleteTopics := []string{
			"transaction-message-in",
			"transaction-message-out",
		}
		do := kafka.SetAdminOperationTimeout(10 * time.Second)
		result, err := a.DeleteTopics(ctx, deleteTopics, do)
		if err != nil {
			fmt.Printf("Error: %v", err)
		}
		fmt.Println(result)
		fmt.Printf("\n\n👋 … and we're done. Slepping 3\n")
		time.Sleep(3 * time.Second)
		fmt.Println("-----------------------------------")
		fmt.Println("Deleting topics in Broker completed")
		fmt.Println("-----------------------------------")
	}
}
