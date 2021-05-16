// Package kafka_manager is a utility package that manages produers and consumers of kafka
// It works on top of segmentio/kafka-go package & simplifies the API significantly.
package kafka_manager

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaManager manages all the producer & consumers for the client
// It trims down the segment-io/kafka-go API significantly.
type KafkaManager struct {
	sync.Mutex
	wg               *sync.WaitGroup
	producer         *kafka.Writer            // Topic to Writer map
	consumers        map[string]*kafka.Reader // Topic to Reader map
	consumerSettings []*ConsumerSettings      // Consumer Setting for Topic
	setting          *Settings                // Global setting for Manager and Producer
	initialized      bool                     // Indicates whether Manager is running
	requeue          chan *kafka.Message      // Channel from which messages are requeue to topic
}

// Settings for Manager
type Settings struct {
	Brokers         []string          // list of Kafka brokers to connect to
	ProducerSetting *ProducerSettings // Producer specific setting
}

// ProducerSettings
// It represents the WriterConfig in segmentio/kafka-go.
// If you need access any other WriterConfig add it here
type ProducerSettings struct {
	Compression  kafka.Compression  // Message Compression type
	RequiredAcks kafka.RequiredAcks // Acknowledgement type
	BatchTimeout time.Duration      // Flush queued after batch timeout
	BatchSize    int                // Number of msgs in a batch
}

// ConsumerSettings
// It represents the ReaderConfig in segmentio/kafka-go.
// If you need access any other ReaderConfig add it here
type ConsumerSettings struct {
	Topic          string        // Topic name
	GroupID        string        // Group ID of the consumer
	Callback       Callback      // Callback handler for processing msg
	CommitInterval time.Duration // Commit messages after
	Offset         int64         // Offset
	ReadBackoffMin time.Duration
	ReadBackoffMax time.Duration
	MinBytes       int // Minimum number of bytes in a batch
	MaxBytes       int // Maximum number of bytes in a batch
}

// Callback handler to read messages from reader ie Kafka
type Callback func(reader *kafka.Reader, wg *sync.WaitGroup)

// DefaultSettings returns default settings
func DefaultSettings() *Settings {
	return &Settings{
		Brokers:         []string{"localhost"},
		ProducerSetting: DefaultProducerSettings(),
	}
}

// DefaultProducerSettings returns default settings
// Use this when you are unsure of which options to set
func DefaultProducerSettings() *ProducerSettings {
	return &ProducerSettings{
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireAll,
		BatchSize:    10,
		BatchTimeout: 20 * time.Millisecond,
	}
}

// DefaultConsumerSettings returns default settings
// Use this when you are unsure of which options to set
func DefaultConsumerSettings(topic string, callback Callback) *ConsumerSettings {
	return &ConsumerSettings{
		Topic:          topic,
		CommitInterval: 0,
		Callback:       callback,
		Offset:         kafka.FirstOffset,
		ReadBackoffMin: 100 * time.Millisecond,
		ReadBackoffMax: time.Second,
		MinBytes:       1,
		MaxBytes:       1e6, // 1 MB
	}
}

// New returns a new KafkaManager initialized with client specific settings
// If you are unsure of which settings to use, call kafkamgr.New(kafkamgr.DefaultSettings())
func New(s *Settings) (*KafkaManager, error) {
	if err := validateSettings(s); err != nil {
		return nil, err
	}
	mgr := &KafkaManager{
		Mutex:            sync.Mutex{},
		wg:               &sync.WaitGroup{},
		consumers:        make(map[string]*kafka.Reader),
		consumerSettings: make([]*ConsumerSettings, 0, 0),
		setting:          s,
		requeue:          make(chan *kafka.Message),
	}
	mgr.producer = mgr.getNewWriter()
	return mgr, nil
}

// AddConsumer adds a new consumer to consume from a Kafka topic
func (f *KafkaManager) AddConsumer(c *ConsumerSettings) error {
	if f.initialized {
		// Can not add new consumers, while manager is already running
		return ErrKafkaManagerAlreadyInitialized
	}

	if err := validateConsumerSetting(c); err != nil {
		return err
	}
	f.consumerSettings = append(f.consumerSettings, c)
	if err := f.initConsumer(c); err != nil {
		return err
	}
	return nil
}

func (f *KafkaManager) initConsumer(c *ConsumerSettings) error {
	_, ok := f.consumers[c.Topic]
	if ok {
		return ErrTopicAlreadyInitialized
	}
	f.consumers[c.Topic] = f.getNewReader(c)
	return nil
}

// Produce adds a message to a given Kafka topic
func (f *KafkaManager) Produce(ctx context.Context, topic, key string, msg []byte) (bool, error) {
	f.Lock()
	var err error
	{
		err = f.producer.WriteMessages(ctx, kafka.Message{
			Topic: topic,
			Key:   bytes.NewBufferString(key).Bytes(),
			Value: msg,
		})
	}
	f.Unlock()
	if err != nil {
		er, ok := err.(*kafka.Error)
		if ok {
			return er.Temporary(), er
		}
		return false, err
	}
	return false, nil
}

// Consume starts consumers
// It blocks until all the consumers are done consuming message
func (f *KafkaManager) Consume() {
	f.initialized = true

	go func(requeue <-chan *kafka.Message, mgr *KafkaManager) {
		for msg := range requeue {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			go mgr.Produce(ctx, msg.Topic, string(msg.Key), msg.Value)
		}
	}(f.requeue, f)

	for _, c := range f.consumerSettings {
		reader, ok := f.consumers[c.Topic]
		if ok {
			f.wg.Add(1)
			go c.Callback(reader, f.wg)
		}
	}
	f.wg.Wait()
}

// Close closes all the open connections from producer & consumers
func (f *KafkaManager) Close(ctx context.Context) {
	if err := f.producer.Close(); err != nil {
		log.Println("failed to close producer")
	}
	for _, c := range f.consumers {
		if err := c.Close(); err != nil {
			log.Println("failed to close consumer")
		}
	}
	close(f.requeue)
}

func (f *KafkaManager) getNewWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(f.setting.Brokers...),
		Compression:  f.setting.ProducerSetting.Compression,
		RequiredAcks: f.setting.ProducerSetting.RequiredAcks,
		Balancer:     kafka.CRC32Balancer{}, // Makes sure msg with a key is sent to the same partition
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
		BatchTimeout: f.setting.ProducerSetting.BatchTimeout,
		BatchSize:    f.setting.ProducerSetting.BatchSize,
		Async:        false,
	}
}

func (f *KafkaManager) getNewReader(s *ConsumerSettings) *kafka.Reader {
	switch s.Offset {
	case kafka.FirstOffset:
	default:
		s.Offset = kafka.LastOffset
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Topic:             s.Topic,
		GroupID:           s.GroupID,
		Brokers:           f.setting.Brokers,
		CommitInterval:    s.CommitInterval,
		StartOffset:       s.Offset,
		ReadBackoffMin:    s.ReadBackoffMin,
		ReadBackoffMax:    s.ReadBackoffMax,
		MinBytes:          s.MinBytes,
		MaxBytes:          s.MaxBytes,
		HeartbeatInterval: 4 * time.Second,
	})
}
