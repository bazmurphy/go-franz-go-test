package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	log.Printf("producer starting...")

	seeds := []string{"localhost:9092"}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		log.Fatalf("error: failed to create new producer: %v\n", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		topic := "my-topic"

		for {
			select {
			case <-signalChannel:
				log.Println("received signal, cancelling context, shutting down producer...")
				cancel()
				return
			default:
				uuid := uuid.NewString()
				key := []byte(uuid)

				randomValue := rand.Intn(100)
				valueComposite := fmt.Sprintf("my-value-%d", randomValue)
				value := []byte(valueComposite)

				record := &kgo.Record{Topic: topic, Key: key, Value: value}

				client.Produce(ctx, record, func(_ *kgo.Record, err error) {
					if err != nil {
						log.Printf("error: failed to produce record: %v\n", err)
					}
					log.Printf("produced record:\n\tkey:%s\n\tvalue:%s\n\ttopic:%s\n\tpartition:%d\n\toffset:%d\n", record.Key, record.Value, record.Topic, record.Partition, record.Offset)
				})

				time.Sleep(1 * time.Second)
			}
		}
	}()

	wg.Wait()
}
