package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"crypto/rand"

	"vinr.eu/snapflux/internal/storage"
)

type msgDoc struct {
	ID          string    `bson:"_id"`
	Topic       string    `bson:"topic"`
	Key         string    `bson:"key"`
	PayloadJSON string    `bson:"payloadJson"`
	CreatedAt   time.Time `bson:"createdAt"`
}

type deliveryDoc struct {
	ID          string                 `bson:"_id"`
	MessageID   string                 `bson:"messageId"`
	Topic       string                 `bson:"topic"`
	Group       string                 `bson:"group"`
	Status      storage.DeliveryStatus `bson:"status"`
	VisibleAt   time.Time              `bson:"visibleAt"`
	Attempts    int                    `bson:"attempts"`
	MaxAttempts int                    `bson:"maxAttempts"`
	CreatedAt   time.Time              `bson:"createdAt"`
}

type brokerDoc struct {
	ID            string    `bson:"_id"`
	Host          string    `bson:"host"`
	Address       string    `bson:"address"`
	Status        string    `bson:"status"`
	LastHeartbeat time.Time `bson:"lastHeartbeat"`
}

type Provider struct {
	client        *mongo.Client
	db            *mongo.Database
	brokers       *mongo.Collection
	messages      *mongo.Collection
	deliveries    *mongo.Collection
	subscriptions *mongo.Collection

	bgCtx    context.Context
	bgCancel context.CancelFunc
}

func New() *Provider {
	ctx, cancel := context.WithCancel(context.Background())
	return &Provider{bgCtx: ctx, bgCancel: cancel}
}

func (p *Provider) Connect(ctx context.Context, url string) error {
	client, err := mongo.Connect(options.Client().ApplyURI(url))
	if err != nil {
		return err
	}
	p.client = client
	p.db = client.Database("snapflux")
	p.brokers = p.db.Collection("brokers")
	p.messages = p.db.Collection("messages")
	p.deliveries = p.db.Collection("deliveries")
	p.subscriptions = p.db.Collection("subscriptions")

	ttlSeconds := int32(604800)
	if v := os.Getenv("MESSAGE_TTL_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ttlSeconds = int32(n)
		}
	}

	if err := p.createIndexes(ctx, ttlSeconds); err != nil {
		return err
	}
	slog.Info("connected to MongoDB")
	return nil
}

func (p *Provider) createIndexes(ctx context.Context, messageTTLSeconds int32) error {
	_, err := p.messages.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "topic", Value: 1}}},
		{Keys: bson.D{{Key: "createdAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(messageTTLSeconds)},
	})
	if err != nil {
		return err
	}
	_, err = p.deliveries.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "topic", Value: 1}, {Key: "group", Value: 1}, {Key: "status", Value: 1}, {Key: "visibleAt", Value: 1}}},
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "visibleAt", Value: 1}}},
	})
	if err != nil {
		return err
	}
	_, err = p.subscriptions.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "topic", Value: 1}, {Key: "group", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	return err
}

func (p *Provider) Close(ctx context.Context) error {
	p.bgCancel()
	if p.client != nil {
		return p.client.Disconnect(ctx)
	}
	return nil
}

func (p *Provider) Ping(ctx context.Context) error {
	return p.db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err()
}

func (p *Provider) InitBroker(ctx context.Context, id, host, address string) error {
	_, err := p.brokers.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "host", Value: host},
			{Key: "address", Value: address},
			{Key: "status", Value: "online"},
			{Key: "lastHeartbeat", Value: time.Now()},
		}}},
		options.UpdateOne().SetUpsert(true))
	return err
}

func (p *Provider) RemoveBroker(ctx context.Context, brokerID string) error {
	_, err := p.brokers.DeleteOne(ctx, bson.D{{Key: "_id", Value: brokerID}})
	return err
}

func (p *Provider) UpdateHeartbeat(ctx context.Context, brokerID string) error {
	_, err := p.brokers.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: brokerID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "lastHeartbeat", Value: time.Now()}}}},
	)
	return err
}

func (p *Provider) EvictStaleBrokers(ctx context.Context, before time.Time, excludeID string) ([]string, error) {
	filter := bson.D{
		{Key: "lastHeartbeat", Value: bson.D{{Key: "$lt", Value: before}}},
		{Key: "_id", Value: bson.D{{Key: "$ne", Value: excludeID}}},
	}
	cur, err := p.brokers.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	var docs []brokerDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	_, err = p.brokers.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	return ids, err
}

func (p *Provider) GetActiveBrokers(ctx context.Context) ([]storage.BrokerEntity, error) {
	cur, err := p.brokers.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var docs []brokerDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	result := make([]storage.BrokerEntity, len(docs))
	for i, d := range docs {
		result[i] = storage.BrokerEntity{ID: d.ID, Host: d.Host, Address: d.Address, Status: d.Status, LastHeartbeat: d.LastHeartbeat}
	}
	return result, nil
}

func (p *Provider) SendMessage(ctx context.Context, id, topic, key string, body any) error {
	payloadJSON, _ := json.Marshal(body)
	doc := msgDoc{ID: id, Topic: topic, Key: key, PayloadJSON: string(payloadJSON), CreatedAt: time.Now()}
	_, err := p.messages.InsertOne(ctx, doc)
	return err
}

func (p *Provider) BulkSendMessages(ctx context.Context, messages []storage.MessageEntity) error {
	if len(messages) == 0 {
		return nil
	}
	docs := make([]interface{}, len(messages))
	for i, m := range messages {
		payloadJSON, _ := json.Marshal(m.Payload)
		docs[i] = msgDoc{ID: m.ID, Topic: m.Topic, Key: m.Key, PayloadJSON: string(payloadJSON), CreatedAt: m.CreatedAt}
	}
	_, err := p.messages.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	return ignoreDuplicates(err)
}

func (p *Provider) BulkCreateDeliveries(ctx context.Context, deliveries []storage.DeliveryEntity) error {
	if len(deliveries) == 0 {
		return nil
	}
	docs := make([]interface{}, len(deliveries))
	for i, d := range deliveries {
		docs[i] = deliveryDoc{
			ID: d.ID, MessageID: d.MessageID, Topic: d.Topic, Group: d.Group,
			Status: d.Status, VisibleAt: d.VisibleAt, Attempts: d.Attempts,
			MaxAttempts: d.MaxAttempts, CreatedAt: d.CreatedAt,
		}
	}
	_, err := p.deliveries.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	return ignoreDuplicates(err)
}

func (p *Provider) GetSubscriptions(ctx context.Context, topic string) ([]string, error) {
	type subDoc struct {
		Group string `bson:"group"`
	}
	cur, err := p.subscriptions.Find(ctx, bson.D{{Key: "topic", Value: topic}})
	if err != nil {
		return nil, err
	}
	var docs []subDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	result := make([]string, len(docs))
	for i, d := range docs {
		result[i] = d.Group
	}
	return result, nil
}

func (p *Provider) UpsertSubscription(ctx context.Context, topic, group string) error {
	type subDoc struct {
		Topic     string    `bson:"topic"`
		Group     string    `bson:"group"`
		CreatedAt time.Time `bson:"createdAt"`
	}
	_, err := p.subscriptions.UpdateOne(ctx,
		bson.D{{Key: "topic", Value: topic}, {Key: "group", Value: group}},
		bson.D{{Key: "$setOnInsert", Value: subDoc{Topic: topic, Group: group, CreatedAt: time.Now()}}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (p *Provider) CreateDeliveries(ctx context.Context, messageID, topic string, groups []string, maxAttempts int) error {
	now := time.Now()
	docs := make([]interface{}, len(groups))
	for i, group := range groups {
		docs[i] = deliveryDoc{
			ID: rand.Text(), MessageID: messageID, Topic: topic, Group: group,
			Status: storage.StatusPending, VisibleAt: now,
			Attempts: 0, MaxAttempts: maxAttempts, CreatedAt: now,
		}
	}
	_, err := p.deliveries.InsertMany(ctx, docs)
	return err
}

func (p *Provider) ReceiveDeliveries(ctx context.Context, topic, group string, limit, visibilityMs int) ([]storage.DeliveryWithMessage, error) {
	now := time.Now()
	visibleAt := now.Add(time.Duration(visibilityMs) * time.Millisecond)

	filter := bson.D{
		{Key: "topic", Value: topic},
		{Key: "group", Value: group},
		{Key: "status", Value: string(storage.StatusPending)},
		{Key: "visibleAt", Value: bson.D{{Key: "$lte", Value: now}}},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{{Key: "status", Value: string(storage.StatusInflight)}, {Key: "visibleAt", Value: visibleAt}}},
		{Key: "$inc", Value: bson.D{{Key: "attempts", Value: 1}}},
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var results []storage.DeliveryWithMessage
	for i := 0; i < limit; i++ {
		var d deliveryDoc
		err := p.deliveries.FindOneAndUpdate(ctx, filter, update, opts).Decode(&d)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break
		}
		if err != nil {
			return nil, err
		}
		var m msgDoc
		err = p.messages.FindOne(ctx, bson.D{{Key: "_id", Value: d.MessageID}}).Decode(&m)
		if err != nil {
			continue
		}
		var body any
		_ = json.Unmarshal([]byte(m.PayloadJSON), &body)
		results = append(results, storage.DeliveryWithMessage{
			ReceiptID: d.ID, MessageID: m.ID, Topic: m.Topic,
			Key: m.Key, Body: body, Attempts: d.Attempts, SentAt: m.CreatedAt,
		})
	}
	return results, nil
}

func (p *Provider) AckDelivery(ctx context.Context, receiptID string) (bool, error) {
	res, err := p.deliveries.DeleteOne(ctx, bson.D{
		{Key: "_id", Value: receiptID},
		{Key: "status", Value: string(storage.StatusInflight)},
	})
	if err != nil {
		return false, err
	}
	return res.DeletedCount == 1, nil
}

func (p *Provider) RequeueStaleDeliveries(ctx context.Context, before time.Time) (int, error) {
	deadFilter := bson.D{
		{Key: "status", Value: string(storage.StatusInflight)},
		{Key: "visibleAt", Value: bson.D{{Key: "$lte", Value: before}}},
		{Key: "$expr", Value: bson.D{{Key: "$gte", Value: bson.A{"$attempts", "$maxAttempts"}}}},
	}
	_, err := p.deliveries.UpdateMany(ctx, deadFilter,
		bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: string(storage.StatusDead)}}}})
	if err != nil {
		return 0, err
	}

	requeueFilter := bson.D{
		{Key: "status", Value: string(storage.StatusInflight)},
		{Key: "visibleAt", Value: bson.D{{Key: "$lte", Value: before}}},
	}
	res, err := p.deliveries.UpdateMany(ctx, requeueFilter, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: string(storage.StatusPending)},
			{Key: "visibleAt", Value: time.Time{}},
		}},
	})
	if err != nil {
		return 0, err
	}
	return int(res.ModifiedCount), nil
}

func (p *Provider) SubscribeToInserts(callback func()) (func(), error) {
	pipeline := mongo.Pipeline{bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}}}
	cs, err := p.messages.Watch(p.bgCtx, pipeline)
	if err != nil {
		slog.Warn("change streams unavailable — falling back to timer-only requeue", "error", err)
		return func() {}, nil
	}

	ctx, cancel := context.WithCancel(p.bgCtx)
	go func() {
		defer func() { _ = cs.Close(context.Background()) }()
		for cs.Next(ctx) {
			callback()
		}
	}()

	return cancel, nil
}

func ignoreDuplicates(err error) error {
	if err == nil {
		return nil
	}
	if bwe, ok := errors.AsType[mongo.BulkWriteException](err); ok {
		for _, we := range bwe.WriteErrors {
			if we.Code != 11000 {
				return err
			}
		}
		return nil
	}
	return err
}
