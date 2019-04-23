package broker

import (
	. "ccsdsmo-malgo/com/event"
	. "ccsdsmo-malgo/mal"
	. "ccsdsmo-malgo/mal/broker"
	"fmt"

	. "ccsdsmo-malgo/mal/api"
	. "ccsdsmo-malgo/mal/encoding/binary"

	// Init TCP transport
	_ "ccsdsmo-malgo/mal/transport/tcp"
	// Blank imports to register all the mal and com elements
)

var (
	broker *BrokerHandler
)

func LaunchBroker(brokerURL string) (*BrokerHandler, error) {

	// Creates the broker
	bro_ctx, err := NewContext(brokerURL)
	if err != nil {
		return nil, err
	}

	bro_cctx, err := NewClientContext(bro_ctx, "broker")
	if err != nil {
		return nil, err
	}

	factory := new(FixedBinaryEncoding)
	updtHandler := NewEventUpdateValueHandler()

	broker, err = NewBroker(bro_cctx, updtHandler, factory)
	if err != nil {
		return nil, err
	}
	fmt.Println("\n-- Broker successfully created --\n")
	// Close the broker at the end of the function
	return broker, nil
}
