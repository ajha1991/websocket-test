package websocket

import (
	"github.com/gobwas/ws"
	_ "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	_ "github.com/gobwas/ws/wsutil"
	"log"
	"net"
	"net/http"
)

type subsPayload struct {
	symbols     []string
	tickChannel chan string
}

type mdsServer struct {
	subsConnMap     map[string][]chan string
	subsPayloadChan chan subsPayload
	tokenChan       chan string
}

func New() (*mdsServer, error) {
	mdsServer := mdsServer{}
	mdsServer.tokenChan = make(chan string, 1000000)
	mdsServer.subsConnMap = make(map[string][]chan string)
	mdsServer.subsPayloadChan = make(chan subsPayload, 10000)
	return &mdsServer, nil
}

func (mdsServer *mdsServer) Init() {
	go mdsServer.updateMap()
	go mdsServer.communicateTick()

}

func (mdsServer *mdsServer) connect(w http.ResponseWriter, r *http.Request) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	go mdsServer.readData(conn)
}

func (mdsServer *mdsServer) readData(conn net.Conn) {
	defer conn.Close()
	subsMap := make(map[string]bool)
	tickChannel := make(chan string, 100000)

	go mdsServer.writeTickToClient(conn, tickChannel)
	for {
		msg, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			log.Printf("Error while reading message for connection %v", err)
			return
		}
		if len(msg) > 0 {
			if _, exist := subsMap["test"]; !exist {
				mdsServer.subsPayloadChan <- subsPayload{[]string{"test"}, tickChannel}
			}
		}
	}
}

func (mdsServer *mdsServer) updateMap() {
	for val := range mdsServer.subsPayloadChan {
		for _, token := range val.symbols {
			if _, exist := mdsServer.subsConnMap[token]; !exist {
				mdsServer.subsConnMap[token] = make([]chan string, 0)
			}
			mdsServer.subsConnMap[token] = append(mdsServer.subsConnMap[token], val.tickChannel)
		}
	}
}

func (mdsServer *mdsServer) SendTick(token string) {
	mdsServer.tokenChan <- token
}

func (mdsServer *mdsServer) communicateTick() {
	for val := range mdsServer.tokenChan {
		if chans, exist := mdsServer.subsConnMap[val]; exist {
			for _, clientChan := range chans {
				clientChan <- val
			}
		}
	}
}

func (mdsServer *mdsServer) writeTickToClient(conn net.Conn, tickChan chan string) {
	defer close(tickChan)
	for val := range tickChan {
		err := wsutil.WriteServerText(conn, []byte(val))
		if err != nil {
			log.Printf("failed to write message %v", err)
		}
	}
}
