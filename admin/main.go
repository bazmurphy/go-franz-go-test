package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	seeds := []string{"localhost:9092"}

	var adminClient *kadm.Client
	{
		client, err := kgo.NewClient(
			kgo.SeedBrokers(seeds...),
		)
		if err != nil {
			log.Fatalf("error: failed to create new admin client: %v\n", err)
		}
		defer client.Close()

		adminClient = kadm.NewClient(client)
	}

	ctx := context.Background()

	topicName := "my-topic"
	partitions := int32(1)
	replicationFactor := int16(1)
	configs := make(map[string]*string)

	response, err := adminClient.CreateTopic(ctx, partitions, replicationFactor, configs, topicName)
	if err != nil {
		log.Fatalf("error: failed to create topic %s: %v\n", topicName, response.Err)
	}
	log.Printf("success: topic '%s' created\n", topicName)
}
