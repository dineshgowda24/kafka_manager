package kafka_manager

import "errors"

var (
	ErrKafkaManagerAlreadyInitialized = errors.New("KafkaManager already running")
	ErrTopicNotInitialized            = errors.New("topic not found")
	ErrTopicAlreadyInitialized        = errors.New("topic already initialized")
	ErrEmptyTopic                     = errors.New("topic can not be empty")
)
