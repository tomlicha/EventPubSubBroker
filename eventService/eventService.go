package service

import (
	"bufio"
	. "ccsdsmo-malgo/com"

	. "ccsdsmo-malgo/com/event"
	. "ccsdsmo-malgo/mal"

	. "EventProvider/broker"
	. "ccsdsmo-malgo/mal/api"
	. "ccsdsmo-malgo/mal/broker"

	. "ccsdsmo-malgo/mal/encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	// Init TCP transport
	_ "ccsdsmo-malgo/mal/transport/tcp"

	// Blank imports to register all the mal and com elements
	. "EventProvider/constants"
	. "EventProvider/service"
)

const (
	brokerURL     = "maltcp://127.0.0.1:12401"
	subscriberURL = "maltcp://127.0.0.1:12402"
	publisherURL  = "maltcp://127.0.0.1:12403"
)

var (
	ekpub1 *EntityKey
	ekpub2 *EntityKey
	pubop  *EventProvider
	subop  *EventConsumer
	subid  Identifier
)

// EventService : TODO:
type EventService struct {
	AreaIdentifier    Identifier
	ServiceIdentifier Identifier
	AreaNumber        UShort
	ServiceNumber     Integer
	AreaVersion       UOctet
	Broker            *BrokerHandler
	running           bool
	wg                sync.WaitGroup
}

// CreateService : TODO:
func (*EventService) CreateService() Service {
	EventService := &EventService{
		AreaIdentifier:    EVENT_SERVICE_AREA_IDENTIFIER,
		ServiceIdentifier: EVENT_SERVICE_SERVICE_IDENTIFIER,
		AreaNumber:        COM_AREA_NUMBER,
		ServiceNumber:     EVENT_SERVICE_SERVICE_NUMBER,
		AreaVersion:       COM_AREA_VERSION,
		running:           true,
		Broker:            nil,
		wg:                *new(sync.WaitGroup),
	}

	return EventService
}

func (eventService *EventService) StartProvider(publisherURL string) error {
	eventService.wg.Add(2)
	// Start the retrieve provider
	go eventService.launchProvider(publisherURL)
	// Start a simple method to stop the providers
	go eventService.stopProviders()
	// Wait until the end of the six operations
	eventService.wg.Wait()

	return nil
}

// stopProviders Stop the providers
func (eventService *EventService) stopProviders() {
	// Inform the WaitGroup that this goroutine is finished at the end of this function
	defer eventService.wg.Done()
	// Wait a little bit
	fmt.Println(eventService.running)
	time.Sleep(200 * time.Millisecond)
	for eventService.running {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Publish ? [Yes/No] ")
		text, _ := reader.ReadString('\n')
		stop := strings.TrimRight(text, "\n")
		if len(stop) > 0 && strings.ToLower(stop)[0] == []byte("y")[0] {
			publishUpdate(ekpub1, ekpub2, pubop, subop, eventService.Broker, publisherURL, subid)
			fmt.Print("Stop event providers ? [Yes/No] ")
			text, _ := reader.ReadString('\n')
			stop := strings.TrimRight(text, "\n")
			if len(stop) > 0 && strings.ToLower(stop)[0] == []byte("y")[0] {
				// Good bye
				eventService.running = false
			}
		}
	}
}

// launchSpecificProvider Start a provider for a specific operation
func (eventService *EventService) launchProvider(publisherURL string) error {
	// Inform the WaitGroup that this goroutine is finished at the end of this function
	defer eventService.wg.Done()

	eventService.Broker, _ = LaunchBroker(brokerURL)

	// Close the provider at the end of the function
	eventService.LaunchPublisher(publisherURL)

	eventService.LaunchSubscriber(subscriberURL)

	return nil
}

func publishUpdate(ekpub1 *EntityKey, ekpub2 *EntityKey, provider *EventProvider, consumer *EventConsumer, Broker *BrokerHandler, publisherURL string, subId Identifier) {
	// Publish a first update

	updthdr1 := &UpdateHeader{*TimeNow(), *NewURI(publisherURL), MAL_UPDATETYPE_CREATION, *ekpub1}
	updthdr2 := &UpdateHeader{*TimeNow(), *NewURI(publisherURL), MAL_UPDATETYPE_CREATION, *ekpub2}
	updthdr3 := &UpdateHeader{*TimeNow(), *NewURI(publisherURL), MAL_UPDATETYPE_CREATION, *ekpub1}
	updtHdrlist1 := UpdateHeaderList([]*UpdateHeader{updthdr1, updthdr2, updthdr3})

	objectType := ObjectType{
		Area:    UShort(2),
		Service: UShort(1),
		Version: UOctet(1),
		Number:  UShort(1),
	}
	var identifierList = IdentifierList([]*Identifier{NewIdentifier("fr"), NewIdentifier("cnes"), NewIdentifier("eventservice"), NewIdentifier("test")})

	// Variables for ArchiveDetailsList
	var objectKey = ObjectKey{
		Domain: identifierList,
		InstId: Long(0),
	}

	var objectID = ObjectId{
		Type: &objectType,
		Key:  &objectKey,
	}
	var objectDetails = ObjectDetails{
		Related: NewLong(0),
		Source:  &objectID,
	}
	updtDetails := &objectDetails
	updtDetailsList := ObjectDetailsList([]*ObjectDetails{updtDetails, updtDetails, updtDetails})

	updt1 := &Blob{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	updt2 := &Blob{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	updt3 := &Blob{0, 1}
	updtlist1 := BlobList([]*Blob{updt1, updt2, updt3})

	err := provider.MonitorEventPublish(&updtHdrlist1, &updtDetailsList, &updtlist1)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("event published")

	// Try to get Notify
	message, identifier, updateHeaderList, objectDetailsList, ElementList, err := consumer.MonitorEventGetNotify()
	fmt.Printf("\t&&&&& Subscriber notified: %d\n", identifier)

	fmt.Printf("\t&&&&& Subscriber notified: OK, %s \n\t%+v \n\t%#v\n\n", identifier, updateHeaderList, ElementList, objectDetailsList)
	fmt.Println(message)

	defer provider.Close()
	defer consumer.Close()
	defer Broker.Close()
	// Deregisters publisher

	// Deregisters subscriber
	idlist := IdentifierList([]*Identifier{&subId})
	consumer.MonitorEventDeregister(idlist)
	provider.MonitorEventDeregister()

	// Waits for socket close
}

func (eventService *EventService) LaunchSubscriber(subscriberURL string) {
	factory := new(FixedBinaryEncoding)

	// Creates the subscriber and registers it
	sub_ctx, _ := NewContext(subscriberURL)

	subscriber, _ := NewClientContext(sub_ctx, "subscriber")

	subscriber.SetDomain(IdentifierList([]*Identifier{NewIdentifier("fr"), NewIdentifier("cnes"), NewIdentifier("eventservice")}))
	subop, _ = NewEventConsumer(factory, subscriber, eventService.Broker.Uri())

	domains := IdentifierList([]*Identifier{NewIdentifier("*")})
	eksub := &EntityKey{NewIdentifier("key1"), NewLong(0), NewLong(0), NewLong(0)}
	var erlist = EntityRequestList([]*EntityRequest{
		&EntityRequest{
			&domains, true, true, true, true, EntityKeyList([]*EntityKey{eksub}),
		},
	})

	var subid = Identifier("MySubscription")
	subs := &Subscription{subid, erlist}
	_ = subop.MonitorEventRegister(*subs)

	fmt.Printf("subop.Register OK\n")

}

func (eventService *EventService) LaunchPublisher(publisherURL string) {
	factory := new(FixedBinaryEncoding)

	// Creates the publisher and registers it
	pub_ctx, _ := NewContext(publisherURL)

	publisher, _ := NewClientContext(pub_ctx, "publisher")
	pubop, _ = NewEventProvider(factory, publisher, eventService.Broker.Uri())
	publisher.SetDomain(IdentifierList([]*Identifier{NewIdentifier("fr"), NewIdentifier("cnes"), NewIdentifier("eventservice"), NewIdentifier("test")}))

	ekpub1 = &EntityKey{NewIdentifier("key1"), NewLong(1), NewLong(1), NewLong(1)}
	ekpub2 = &EntityKey{NewIdentifier("key2"), NewLong(1), NewLong(1), NewLong(1)}
	var eklist = EntityKeyList([]*EntityKey{ekpub1, ekpub2})

	pubop.MonitorEventRegister(&eklist)

	fmt.Printf("pubop.Register OK\n")

}
