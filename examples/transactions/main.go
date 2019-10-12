package main

import (
	"github.com/davecgh/go-spew/spew"
	"log"

	"github.com/Shopify/sarama"
)

var (
	brokers         = []string{"localhost:9092"}
	transactionalID = "my-consumer-0"
	topic           = "test-topic"
	commit          = true
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Version = sarama.V1_1_1_0
	config.Net.MaxOpenRequests = 1
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.TransactionalID = &transactionalID

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Panic(err)
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Panic(err)
	}

	controller, err := client.Controller()
	if err != nil {
		log.Panic(err)
	}

	var transactionCoordinator *sarama.Broker
	{
		result, err := controller.FindCoordinator(&sarama.FindCoordinatorRequest{
			Version:         1,
			CoordinatorKey:  transactionalID,
			CoordinatorType: sarama.CoordinatorTransaction,
		})
		if err != nil {
			log.Panic(err)
		}
		if result.Err != sarama.ErrNoError {
			log.Println(result.Err)
			log.Panic(result.ErrMsg)
		}
		transactionCoordinator = result.Coordinator
		err = transactionCoordinator.Open(config)
		if err != nil {
			log.Panic(err)
		}
		log.Println(transactionCoordinator.Connected())
	}

	transactionalManager := producer.GetTransactionalManager()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("some random message"),
	}
	partition, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Panic(err)
	}

	// Add consumer offsets to transaction
	// {
	// 	addOffsetResponse, err := controller.AddOffsetsToTxn(&sarama.AddOffsetsToTxnRequest{
	// 		ProducerID:      transactionalManager.GetProducerID(),
	// 		ProducerEpoch:   transactionalManager.GetProducerEpoch(),
	// 		TransactionalID: transactionalID,
	// 		GroupID:         consumerGroup,
	// 	})
	// 	if err != nil {
	// 		log.Panic(err)
	// 	}
	// 	if addOffsetResponse.Err != sarama.ErrNoError {
	// 		log.Panic(addOffsetResponse)
	// 	}

	// }

	// AddPartitionsToTxn
	{
		addPartResponse, err := transactionCoordinator.AddPartitionsToTxn(&sarama.AddPartitionsToTxnRequest{
			TransactionalID: transactionalID,
			ProducerID:      transactionalManager.GetProducerID(),
			ProducerEpoch:   transactionalManager.GetProducerEpoch(),
			TopicPartitions: map[string][]int32{topic: {partition}},
		})
		if err != nil {
			log.Panic(err)
		}
		for _, results := range addPartResponse.Errors {
			for _, partitionResult := range results {
				if partitionResult.Err != sarama.ErrNoError {
					spew.Dump(addPartResponse)
					log.Panic()
				}
			}
		}
	}

	{
		endTxnResp, err := transactionCoordinator.EndTxn(&sarama.EndTxnRequest{
			TransactionalID:   transactionalID,
			ProducerEpoch:     transactionalManager.GetProducerEpoch(),
			ProducerID:        transactionalManager.GetProducerID(),
			TransactionResult: commit, // abort or commit
		})
		if err != nil {
			log.Panic(err)
		}
		if endTxnResp.Err != sarama.ErrNoError {
			log.Panic(endTxnResp)
		}
	}
}
