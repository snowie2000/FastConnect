// FastConnect project main.go
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
)

var (
	cm        *ConnMgr
	localPort uint
	server    string
	connNum   int
)

func pipeline(src, dst net.Conn) {
	go func() {
		defer src.Close()
		io.Copy(src, dst)
	}()
	defer dst.Close()
	io.Copy(dst, src)
}

func handleIncomingConn(income net.Conn) {
	outcome := cm.GetConn()
	if outcome != nil {
		pipeline(income, outcome)
	} else {
		log.Println("Can't make connections to remote, giving up.")
		income.Close()
	}
}

func runClient() {
	cm = &ConnMgr{
		server:        server,
		count:         0,
		nMax:          connNum,
		nHeartBeat:    5,
		cList:         NewConnList(),
		connKickstart: make(chan (bool)),
	}
	go cm.Run()
	l, e := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", localPort))
	if e != nil {
		log.Fatal(e)
	}
	for {
		conn, e := l.Accept()
		if e == nil {
			go handleIncomingConn(conn)
		} else {
			log.Fatal(e)
		}
	}
}

func handleServerConn(conn net.Conn) {
	var (
		ptype []byte = make([]byte, 1)
		e     error
	)
wait:
	for {
		_, e = conn.Read(ptype)
		if e != nil {
			log.Println("Pre-connection closed due to", e)
			conn.Close()
			return
		}
		switch ptype[0] {
		case signalKeepalive[0]: // do nothing on keepalive
		case signalStart[0]:
			break wait
		}
	}
	// connection woke up
	remote, err := net.Dial("tcp", server)
	if err == nil {
		pipeline(conn, remote)
	} else {
		log.Println("Can't connect to remote,", err)
		conn.Close()
	}
}

func runServer() {
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", localPort))
	if e != nil {
		log.Fatal(e)
	}
	for {
		conn, e := l.Accept()
		if e == nil {
			go handleServerConn(conn)
		} else {
			log.Println("Can't accept a connection, because of", e)
		}
	}
}

func main() {
	flag.UintVar(&localPort, "l", 0, "Local binding port")
	flag.StringVar(&server, "s", "", "Server address")
	mode := flag.String("m", "client", "Running mode, client or server")
	flag.IntVar(&connNum, "c", 10, "Pre-connected connection limit")
	flag.Parse()
	if localPort == 0 || server == "" || connNum < 1 {
		flag.PrintDefaults()
		return
	}

	if *mode == "client" {
		runClient()
	} else {
		runServer()
	}
}
