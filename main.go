package main

import (
	"fmt"
	"time"
	"websocket-test/websocket"
)

func main() {
	ws, _ := websocket.New()
	go startLoad(ws)
	fmt.Printf("service started")
	ws.Init()
}

func startLoad(ws *websocket.MDSServer) {
	ticker := time.NewTicker(1 * time.Millisecond)
	done := make(chan bool)
	quote1Packet := websocket.Quote1packet{}
	quote1Packet.Symbol = "test"
	quote1Packet.LChange = 1
	quote1Packet.OI = 2
	quote1Packet.ExchangeSegment = 101
	quote1Packet.LastTradedPrice = 123
	quote1Packet.OIChange = 2

	for {
		select {
		case <-done:
			return
		case _ = <-ticker.C:
			for i := 1; i < 2; i++ {
				ws.SendTick(fmt.Sprintf("%s-%d", quote1Packet.Symbol, i), quote1Packet)
			}
		}
	}
}
