package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dineshgowda24/kafka_manager"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{}) // Do not use this in production
	lgr := logrus.StandardLogger()
	mgr := kafka_manager.NewKafkaManager()

	if err := mgr.AddProducer(kafka_manager.DefaultProducerSetting("ping_pong")); err != nil {
		lgr.WithError(err).Fatal("failed adding producer")
	}

	if err := mgr.AddConsumer(&kafka_manager.ConsumerSetting{
		Topic:   "ping_pong",
		GroupID: "ping_pong_grp",
		Brokers: []string{"localhost"},
		Callback: func(reader *kafka.Reader, wg *sync.WaitGroup) {
			lgr.Info("Started to consume")
			count := 0
			for {
				count += 1
				if count > 100 {
					break
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_, err := mgr.Produce(ctx, "ping_pong", []byte(""), []byte("Ping Pong..."))
				if err != nil {
					lgr.WithError(err).Fatal(err)
				}

				msg, err := reader.FetchMessage(context.Background())
				if err != nil {
					lgr.WithError(err).Fatal(err)
				}
				lgr.Info(fmt.Sprintf("Received a new msg with partition [%d] offset [%d] : %s", msg.Partition, msg.Offset, string(msg.Value)))

				cCtx, cCancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cCancel()
				if err := reader.CommitMessages(cCtx, msg); err != nil {
					lgr.WithError(err).Fatal(err)
				}
			}
			wg.Done()
		},
	}); err != nil {
		lgr.WithError(err).Fatal("failed adding consumer")
	}

	mgr.Consume() // blocking call
}
