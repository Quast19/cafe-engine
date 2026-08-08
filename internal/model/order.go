package model

import "time"

type ItemType string

const (
	ItemPizza    ItemType = "PIZZA"
	ItemCoffee   ItemType = "COFFEE"
	ItemDietCoke ItemType = "DIET_COKE"
)

type Order struct {
	ID        string     `json:"id"`
	Items     []ItemType `json:"items"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
}
