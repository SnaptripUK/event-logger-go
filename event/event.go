package event

import (
	"encoding/json"
	"time"
)

type Event struct {
	CreatedAt time.Time
	Cat       string
	Tag       string
	Attrs     map[string]interface{} `bson:"inline"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	inlineMap := make(map[string]interface{})
	inlineMap["created_at"] = e.CreatedAt
	inlineMap["cat"] = e.Cat
	inlineMap["tag"] = e.Tag
	for k, v := range e.Attrs {
		inlineMap[k] = v
	}
	return json.Marshal(inlineMap)
}
