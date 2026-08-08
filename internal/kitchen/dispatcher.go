package kitchen

import (
	"cafe-engine/internal/model"
	"log"
	"time"
)

type Dispatcher struct {
	OrderChan chan model.Order
	NumChefs  int
}

func NewDispatcher(bufferSize int, numChefs int) *Dispatcher {
	return &Dispatcher{
		OrderChan: make(chan model.Order, bufferSize),
		NumChefs:  numChefs,
	}
}

func (d *Dispatcher) Start() {
	for i := 1; i <= d.NumChefs; i++ {
		go d.chefWorker(i, d.OrderChan)
	}
}

// chefWorker is a Goroutine that continuously reads from the channel
func (d *Dispatcher) chefWorker(chefID int, orderChan <-chan model.Order) {
	for order := range orderChan {
		log.Printf("👨‍🍳 [Chef %d] Picked up order %s (Items: %v)", chefID, order.ID, order.Items)

		// Simulate cooking/preparation time based on item type
		for _, item := range order.Items {
			switch item {
			case model.ItemPizza:
				log.Println("🍕 Preparing Pizza...")
				time.Sleep(3 * time.Second)
			case model.ItemCoffee:
				log.Println("☕ Preparing Coffee...")
				time.Sleep(1 * time.Second)
			case model.ItemDietCoke:
				log.Println("🥤 Preparing Diet Coke...")
				time.Sleep(300 * time.Millisecond)
			}
		}

		log.Printf("✅ [Chef %d] COMPLETED order %s", chefID, order.ID)
	}
}
