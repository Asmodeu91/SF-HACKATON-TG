package entities

type Message struct {
	From   string `json:"from"`
	FromId string `json:"from_id"`
}

type Root struct {
	Messages []Message `json:"messages"`
}
