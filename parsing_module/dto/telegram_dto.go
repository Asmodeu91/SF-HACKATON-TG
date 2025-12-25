package dto

type Message struct {
	From   string `json:"from"`
	FromId string `json:"from_id"`
}

type Root struct {
	Messages []Message `json:"messages"`
}

type UserInfo struct {
	UserId       string
	UserName     string
	MessageCount int
	PercentMsg   int
}

type UserInfoSlice []*UserInfo

// Len is part of sort.Interface.
func (slice UserInfoSlice) Len() int {
	return len(slice)
}

// Swap is part of sort.Interface.
func (slice UserInfoSlice) Swap(left, right int) {
	slice[left], slice[right] = slice[right], slice[left]
}

// Less is part of sort.Interface. We use count as the value to sort by
func (slice UserInfoSlice) Less(left, right int) bool {
	return slice[left].MessageCount >= slice[right].MessageCount
}
