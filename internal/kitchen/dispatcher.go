package kitchen

import (
	"log"
	"time"

	"cafe-engine/internal/model"
)

type StatusCallback func(order model.Order, status string)

type Dispatcher struct {
	OrderChan      chan model.Order
	NumChefs       int
	onStatusChange StatusCallback
}

func NewDispatcher(bufferSize int, numChefs int, callback StatusCallback) *Dispatcher {
	return &Dispatcher{
		OrderChan:      make(chan model.Order, bufferSize),
		NumChefs:       numChefs,
		onStatusChange: callback,
	}
}

func (d *Dispatcher) Start() {
	for i := 1; i <= d.NumChefs; i++ {
		go d.chefWorker(i, d.OrderChan)
	}
}

func (d *Dispatcher) chefWorker(chefID int, orderChan <-chan model.Order) {
	for order := range orderChan {
		log.Printf("👨‍🍳 [Chef %d] Picked up order %s", chefID, order.ID)

		// 1. Publish COOKING status when chef picks it up
		if d.onStatusChange != nil {
			d.onStatusChange(order, "COOKING")
		}

		// 2. Perform actual cooking work
		for _, item := range order.Items {
			switch item {
			case model.ItemPizza:
				log.Println("🍕 Preparing Pizza...")
				time.Sleep(3 * time.Minute) // Set to 3s for snappy testing
			case model.ItemCoffee:
				log.Println("☕ Preparing Coffee...")
				time.Sleep(1 * time.Second)
			case model.ItemDietCoke:
				log.Println("🥤 Pouring Diet Coke...")
				time.Sleep(500 * time.Millisecond)
			}
		}

		log.Printf("✅ [Chef %d] COMPLETED order %s", chefID, order.ID)

		// 3. Publish COMPLETED status ONLY AFTER cooking loop finishes!
		if d.onStatusChange != nil {
			d.onStatusChange(order, "COMPLETED")
		}
	}
}
