package storage

import (
	"context"
	"time"
)

type DeliveryStatus string

const (
	StatusPending  DeliveryStatus = "pending"
	StatusInflight DeliveryStatus = "inflight"
	StatusDead     DeliveryStatus = "dead"
)

type MessageEntity struct {
	ID        string
	Topic     string
	Key       string
	Payload   any
	CreatedAt time.Time
}

type DeliveryEntity struct {
	ID          string
	MessageID   string
	Topic       string
	Group       string
	Status      DeliveryStatus
	VisibleAt   time.Time
	Attempts    int
	MaxAttempts int
	CreatedAt   time.Time
}

type BrokerEntity struct {
	ID            string
	Host          string
	Address       string
	Status        string
	LastHeartbeat time.Time
}

type DeliveryWithMessage struct {
	ReceiptID string
	MessageID string
	Topic     string
	Key       string
	Body      any
	Attempts  int
	SentAt    time.Time
}

type Provider interface {
	Connect(ctx context.Context, url string) error
	Close(ctx context.Context) error
	Ping(ctx context.Context) error

	InitBroker(ctx context.Context, id, host, address string) error
	RemoveBroker(ctx context.Context, brokerID string) error
	UpdateHeartbeat(ctx context.Context, brokerID string) error
	EvictStaleBrokers(ctx context.Context, before time.Time, excludeID string) ([]string, error)
	GetActiveBrokers(ctx context.Context) ([]BrokerEntity, error)

	SendMessage(ctx context.Context, id, topic, key string, body any) error
	BulkSendMessages(ctx context.Context, messages []MessageEntity) error
	BulkCreateDeliveries(ctx context.Context, deliveries []DeliveryEntity) error

	GetSubscriptions(ctx context.Context, topic string) ([]string, error)
	UpsertSubscription(ctx context.Context, topic, group string) error

	CreateDeliveries(ctx context.Context, messageID, topic string, groups []string, maxAttempts int) error
	ReceiveDeliveries(ctx context.Context, topic, group string, limit, visibilityMs int) ([]DeliveryWithMessage, error)
	AckDelivery(ctx context.Context, receiptID string) (bool, error)
	RequeueStaleDeliveries(ctx context.Context, before time.Time) (int, error)

	SubscribeToInserts(callback func()) (func(), error)
}
