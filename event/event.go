package event

import "time"

type Event struct {
	CreatedAt time.Time
	Name      string
	Attrs     map[string]interface{} `bson:"inline"`
}
