package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	log.Printf("consumer starting...")

	seeds := []string{"localhost:9092"}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("my-group"),
		kgo.ConsumeTopics("my-topic"),
		// kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Fatalf("error: failed to create new consumer: %v\n", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-signalChannel:
			log.Println("received signal, cancelling context, shutting down consumer...")
			cancel()
			return
		default:
			fetches := client.PollFetches(ctx)

			if errs := fetches.Errors(); len(errs) > 0 {
				// TODO: how many times should we fail to fetch before reacting?
				log.Printf("error: consumer fetches errors: %v", errs)
			}

			iterator := fetches.RecordIter()
			for !iterator.Done() {
				record := iterator.Next()
				log.Printf("consumed record:\n\tkey:%s\n\tvalue:%s\n\ttopic:%s\n\tpartition:%d\n\toffset:%d\n", record.Key, record.Value, record.Topic, record.Partition, record.Offset)
			}
		}
	}
}
