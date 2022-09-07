package websocket

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	//_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
)

var maxPacketsPerClient int = 1000

var upgrader = websocket.Upgrader{} // use default options

var addr = flag.String("addr", ":8080", "http service address")

type subsPayload struct {
	symbols  []string
	client   *client
	subsCode int8 //0 for unsubscribe and 1 for subscribe
}

type tickPayload struct {
	token       string
	payload     []byte
	dataType    int8
	subsPayload *subsPayload
}

type MDSServer struct {
	subsConnMap map[string]map[string]*client
	//subsPayloadChan chan subsPayload //for updating subs connection map
	tokenChan    chan *tickPayload
	quote1Map    sync.Map // not using this
	subsMapMutex *sync.Mutex
}

type client struct {
	ID       string
	dataChan chan []byte
	conn     *websocket.Conn
	subsMap  map[string]bool
	lock     *sync.Mutex
}

type Quote1packet struct {
	Symbol          string
	ExchangeSegment int32
	LastTradedPrice int32
	LChange         int32
	OI              int32
	OIChange        int32
}

var connected int

func New() (*MDSServer, error) {
	mdsServer := MDSServer{}
	mdsServer.tokenChan = make(chan *tickPayload, 100000)
	mdsServer.subsConnMap = make(map[string]map[string]*client)
	mdsServer.quote1Map = sync.Map{}
	//mdsServer.subsPayloadChan = make(chan subsPayload, 10000)
	mdsServer.subsMapMutex = &sync.Mutex{}
	return &mdsServer, nil
}

func (mdsServer *MDSServer) Init() {
	mdsServer.createSubsConnMap()
	go mdsServer.readTickPayload()
	http.HandleFunc("/", mdsServer.connect)
	http.ListenAndServe(*addr, nil)

}

func (mdsServer *MDSServer) createSubsConnMap() {
	baseSymbol := "test"
	for i := 1; i < 31; i++ {
		token := fmt.Sprintf("%s-%d", baseSymbol, i)
		mdsServer.subsConnMap[token] = make(map[string]*client)
	}
}

func (mdsServer *MDSServer) connect(w http.ResponseWriter, r *http.Request) {
	connected++
	log.Printf("connection started %d", connected)
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error while upgrading connection %v", err)
		w.WriteHeader(500)
		return
	}
	log.Printf("upgrade complete  %d ", connected)

	uid, _ := uuid.NewUUID()

	client := client{
		ID:       uid.String(),
		dataChan: make(chan []byte, maxPacketsPerClient),
		conn:     conn,
		subsMap:  make(map[string]bool),
		lock:     &sync.Mutex{},
	}

	go mdsServer.readData(&client)
	log.Printf("connection complete  %s - %d ", uid.String(), connected)

}

func (mdsServer *MDSServer) readData(client *client) {
	defer mdsServer.onClose(client)
	go mdsServer.writeTickToClient(client)
	for {
		mType, msg, err := client.conn.ReadMessage()
		if mType == websocket.CloseMessage {
			mdsServer.onClose(client)
			fmt.Printf("received connection close ")
			return
		}
		if err != nil {
			mdsServer.onClose(client)
			log.Printf("Error while reading message for connection %v", err)
			return
		}
		if len(msg) > 0 {
			if _, exist := client.subsMap["test-1"]; !exist {

				baseSymbol := "test"
				allSymbols := make([]string, 0)
				client.lock.Lock()
				for i := 1; i < 31; i++ {
					token := fmt.Sprintf("%s-%d", baseSymbol, i)
					client.subsMap[token] = true
					allSymbols = append(allSymbols, token)
				}
				client.lock.Unlock()
				mdsServer.tokenChan <- &tickPayload{dataType: 1, subsPayload: &subsPayload{allSymbols, client, 1}}
			}
		}
	}
}

func (mdsServer *MDSServer) updateSubsMap(val *tickPayload) {
	if val.subsPayload.subsCode == 0 {
		mdsServer.unsubscribe(val.subsPayload.symbols, val.subsPayload.client)
	} else {
		mdsServer.subscribe(val.subsPayload.symbols, val.subsPayload.client)
	}
}

func (mdsServer *MDSServer) subscribe(symbols []string, ws *client) {
	for _, token := range symbols {
		if _, exist := mdsServer.subsConnMap[token]; exist {
			mdsServer.subsConnMap[token][ws.ID] = ws
		}
	}
}

func (mdsServer *MDSServer) unsubscribe(symbols []string, ws *client) {
	for _, token := range symbols {
		delete(ws.subsMap, token)
		if _, exist := mdsServer.subsConnMap[token]; exist {
			delete(mdsServer.subsConnMap[token], ws.ID)
		}
	}
}

func (mdsServer *MDSServer) SendTick(token string, packet Quote1packet) {
	convertedBytes := convertQuote1ToBytes(packet)
	//mdsServer.quote1Map.Store(token, convertQuote1ToBytes(packet))
	mdsServer.tokenChan <- &tickPayload{
		token:    token,
		payload:  convertedBytes,
		dataType: int8(0),
	}
}

func convertQuote1ToBytes(packet Quote1packet) []byte {
	var buf = bytes.NewBuffer(make([]byte, 22))
	packetSize := int16(22)
	quote1AppCode := uint8(100)
	binary.Write(buf, binary.LittleEndian, packetSize)
	binary.Write(buf, binary.LittleEndian, uint8(0))
	binary.Write(buf, binary.LittleEndian, quote1AppCode)

	symbol, _ := strconv.Atoi(packet.Symbol)
	binary.Write(buf, binary.LittleEndian, int32(symbol))
	binary.Write(buf, binary.LittleEndian, uint8(packet.ExchangeSegment))
	binary.Write(buf, binary.LittleEndian, packet.LastTradedPrice)
	binary.Write(buf, binary.LittleEndian, packet.LChange)
	binary.Write(buf, binary.LittleEndian, packet.OI)
	binary.Write(buf, binary.LittleEndian, packet.OIChange)
	return buf.Bytes()

}

func (mdsServer *MDSServer) readTickPayload() {
	for val := range mdsServer.tokenChan {
		if val.dataType == int8(0) {
			mdsServer.communicateTick(val)
		} else {
			mdsServer.updateSubsMap(val)
		}
	}
}

func (mdsServer *MDSServer) communicateTick(val *tickPayload) {
	if chans, exist := mdsServer.subsConnMap[val.token]; exist {
		for _, client := range chans {
			if len(client.dataChan) < 10000 {
				client.dataChan <- val.payload
			}
		}
	}
}

func (mdsServer *MDSServer) onClose(client *client) {
	client.conn.Close()
	symbols := make([]string, 0)
	client.lock.Lock()
	for k := range client.subsMap {
		symbols = append(symbols, k)
	}
	client.lock.Unlock()
	mdsServer.tokenChan <- &tickPayload{dataType: 1, subsPayload: &subsPayload{symbols, client, 0}}
}

func (mdsServer *MDSServer) writeTickToClient(client *client) {
	//defer close(tickChan)
	for val := range client.dataChan {
		//data, _ := mdsServer.quote1Map.Load(val)
		err := client.conn.WriteMessage(websocket.BinaryMessage, val)
		if err != nil {
			log.Printf("failed to write message %v", err)
			if strings.Contains(err.Error(), "close sent") {
				log.Printf("returning from closed connection")
				mdsServer.onClose(client)
				return
			}

		}
	}
}
