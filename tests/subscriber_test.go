package tests

import (
	"bufio"
	. "ccsdsmo-malgo/com/event"
	. "ccsdsmo-malgo/mal"
	. "ccsdsmo-malgo/mal/encoding/binary"
	"os"
	"strings"
	"sync"
	"time"

	. "ccsdsmo-malgo/mal/api"
	_ "ccsdsmo-malgo/mal/transport/tcp"

	// Needed to initialize TCP transport factory

	"fmt"
	"testing"
)

const (
	brokerURL     = "maltcp://127.0.0.1:12401/broker"
	subscriberURL = "maltcp://127.0.0.1:12408"
)

var (
	running = true
	wg      sync.WaitGroup
	subop   *EventConsumer
	subid   Identifier
)

func launchSubscriber(t *testing.T) {
	defer wg.Done()

	factory := new(FixedBinaryEncoding)

	// Creates the subscriber and registers it
	sub_ctx, err := NewContext(subscriberURL)
	if err != nil {
		t.Fail()
	}
	defer sub_ctx.Close()

	subscriber, err := NewClientContext(sub_ctx, "subscribe2")
	if err != nil {
		t.Fail()
	}

	subscriber.SetDomain(IdentifierList([]*Identifier{NewIdentifier("fr"), NewIdentifier("cnes"), NewIdentifier("eventservice")}))
	subop, err = NewEventConsumer(factory, subscriber, NewURI(brokerURL))
	if err != nil {
		t.Fail()
	}

	domains := IdentifierList([]*Identifier{NewIdentifier("*")})
	eksub := &EntityKey{NewIdentifier("key1"), NewLong(0), NewLong(0), NewLong(0)}
	var erlist = EntityRequestList([]*EntityRequest{
		&EntityRequest{
			&domains, true, true, true, true, EntityKeyList([]*EntityKey{eksub}),
		},
	})

	var subid = Identifier("MySubscription2")
	subs := &Subscription{subid, erlist}
	err = subop.MonitorEventRegister(*subs)
	if err != nil {
		t.Fail()
	}

	fmt.Printf("subop.Register OK\n")

}

func notify(t *testing.T) {
	defer wg.Done()
	time.Sleep(200 * time.Millisecond)

	for running {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Notify ? [Yes/No] ")
		text, _ := reader.ReadString('\n')
		stop := strings.TrimRight(text, "\n")
		if len(stop) > 0 && strings.ToLower(stop)[0] == []byte("y")[0] {
			// Try to get Notify
			message, identifier, updateHeaderList, objectDetailsList, ElementList, err := subop.MonitorEventGetNotify()
			if err != nil {
				t.Fail()
			}
			fmt.Printf("\t&&&&& Subscriber notified: %d\n", identifier)

			fmt.Printf("\t&&&&& Subscriber notified: OK, %s \n\t%+v \n\t%#v\n\n%+v", identifier, updateHeaderList, ElementList, objectDetailsList)
			fmt.Println(message)

			// Deregisters subscriber
			idlist := IdentifierList([]*Identifier{&subid})
			subop.MonitorEventDeregister(idlist)
			running = false
		}
	}
}

func TestSub(t *testing.T) {
	wg = *new(sync.WaitGroup)
	wg.Add(2)
	// Start the retrieve provider
	go launchSubscriber(t)
	// Start a simple method to stop the providers
	//go notify(t)
	// Wait until the end of the six operations
	wg.Wait()

}
