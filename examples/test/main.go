package main

import (
	"log"

	"github.com/Shopify/sarama"
)

var brokers = []string{"localhjost:9092"}
var transactionalID = "my-consumer-0"
var consumerGroup = "my-consumer-group"

func main() {

	config := sarama.NewConfig()
	config.Producer.Idempotent = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	// Let assume the topic and the partition for now // TODO: let's not
	// partitionLeader, err := client.Leader("test-topic", 0)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	controller, err := client.Controller()
	if err != nil {
		log.Fatal(err)
	}

	transactionalManager := producer.GetTransactionalManager()

	// AddPartitionsToTxn
	{
		addPartResponse, err := controller.AddPartitionsToTxn(&sarama.AddPartitionsToTxnRequest{
			TransactionalID: transactionalID,
			ProducerID:      transactionalManager.GetProducerID(),
			ProducerEpoch:   transactionalManager.GetProducerEpoch(),
			TopicPartitions: map[string][]int32{"test-topic": []int32{0}},
		})
		if err != nil {
			log.Fatal(err)
		}
		for _, results := range addPartResponse.Errors {
			for _, partitionResult := range results {
				if partitionResult.Err != sarama.ErrNoError {
					log.Printf("got an error %v on partitions %v", partitionResult.Err, partitionResult.Partition)
					log.Fatal(addPartResponse)
				}
			}
		}
	}

	msg := &sarama.ProducerMessage{}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Fatal(err)
	}

	// AddOffsetsToTxn
	// {
	// 	addOffsetResponse, err := controller.AddOffsetsToTxn(&sarama.AddOffsetsToTxnRequest{
	// 		ProducerID:      transactionalManager.GetProducerID(),
	// 		ProducerEpoch:   transactionalManager.GetProducerEpoch(),
	// 		TransactionalID: transactionalID,
	// 		GroupID:         consumerGroup,
	// 	})
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	if addOffsetResponse.Err != sarama.ErrNoError {
	// 		log.Fatal(addOffsetResponse)
	// 	}

	// }

	{
		endTxnResp, err := controller.EndTxn(&sarama.EndTxnRequest{
			TransactionalID:transactionalID,
			ProducerEpoch: transactionalManager.GetProducerEpoch(),
			ProducerID:transactionalManager.GetProducerID(),
			TransactionResult:true, //commit
		}
		
		)
		if err != nil {
			log.Fatal(err)
		}
		if endTxnResp.Err != sarama.ErrNoError {
			log.Fatal(endTxnResp)
		}
	}
}
