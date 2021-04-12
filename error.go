package kafka_manager

import "errors"

var (
	ErrKafkaManagerAlreadyInitialized = errors.New("KafkaManager already running")
	ErrTopicNotInitialized            = errors.New("topic not found")
)
