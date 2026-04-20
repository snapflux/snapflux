package model

type Durability string

const (
	DurabilityFireAndForget Durability = "fire-and-forget"
	DurabilityDurable       Durability = "durable"
	DurabilityStrict        Durability = "strict"
)

type SendRequest struct {
	Key        string     `json:"key"`
	Body       any        `json:"body"`
	Durability Durability `json:"durability,omitempty"`
}

type SendResponse struct {
	ID         string     `json:"id"`
	Durability Durability `json:"durability,omitempty"`
}

type MessageResponse struct {
	ID        string `json:"id"`
	ReceiptID string `json:"receiptId"`
	Key       string `json:"key"`
	Body      any    `json:"body"`
	Attempts  int    `json:"attempts"`
	SentAt    string `json:"sentAt"`
}

type AckResponse struct {
	ReceiptID    string `json:"receiptId"`
	Acknowledged bool   `json:"acknowledged"`
}

type HealthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}
