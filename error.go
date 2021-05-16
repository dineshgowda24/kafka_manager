package kafka_manager

import "errors"

var (
	ErrKafkaManagerAlreadyInitialized = errors.New("KafkaManager already running")
	ErrTopicAlreadyInitialized        = errors.New("topic already initialized")
	ErrEmptyTopic                     = errors.New("topic can not be empty")
	ErrEmptyBrokers                   = errors.New("Setting.Brokers required")
)
