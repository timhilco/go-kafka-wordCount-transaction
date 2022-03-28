package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func readFile() []string {
	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	logger.Info().Msg(path)
	var fileName = path + "/file1.txt"
	file, err := os.Open(fileName)
	if err != nil {
		text := fmt.Sprintf("Error: %s", err)
		logger.Info().Msg(text)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lines []string

	for scanner.Scan() {
		text := scanner.Text()
		lines = append(lines, text)
	}
	// if err := scanner.Err (); err! = nil {
	// 	text := fmt.Sprintf ("Error: %s", err)
	// }
	return lines
}
func publishSentences(wg *sync.WaitGroup, bootstrapServer string, topic string, times int) {
	logger.Info().Msg("-> Starting Sentence Publishing")
	defer wg.Done()

	var err error
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		// 	"enable.idempotence": true,
		// 	"transactional.id":   "prod-1",
	})

	if err != nil {
		text := fmt.Sprintf("Failed to create producer: %s", err)
		logger.Info().Msg(text)
		os.Exit(1)
	}

	// sentences1 := []string{
	// 	"This is the first sentence",
	// 	"This is the second sentence",
	// }
	for i := 0; i < times; i++ {
		sentences := readFile()
		key := 0
		for _, s := range sentences {
			deliveryChan := make(chan kafka.Event)
			key = key + 1
			keyA := strconv.Itoa(key)
			kafkaMessage := kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(keyA),
				Value:          []byte(s),
			}
			err := producer.Produce(&kafkaMessage, deliveryChan)
			if err != nil {
				text := fmt.Sprintf("Error: %s", err)
				logger.Info().Msg(text)
			}
			pe := <-deliveryChan
			m := pe.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				text := fmt.Sprintf("Delivery failed: %v", m.TopicPartition.Error)
				logger.Info().Msg(text)
			} else {
				//text := fmt.Sprintf("Delivered message to topic %s [%d] at offset %v",
				//	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

			close(deliveryChan)
		}
	}
	logger.Info().Msg("Sentence Publishing Finished")
}
