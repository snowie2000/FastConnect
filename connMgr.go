// connMgr
package main

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

var (
	errorPoolEmpty = errors.New("Connection pool is empty")
)

type HealthCondition int

const (
	hcGood HealthCondition = iota
	hcBad
	hcCanncel
)

type ConnList struct {
	nCount    int
	cHead     *ConnSlot
	cTail     *ConnSlot
	chainLock sync.Locker
	pool      *sync.Pool
}

func NewConnList() *ConnList {
	p := &ConnSlot{nil, nil}
	return &ConnList{
		nCount:    0,
		cHead:     p,
		cTail:     p,
		chainLock: &sync.Mutex{},
		pool: &sync.Pool{
			New: func() interface{} {
				return &ConnSlot{nil, nil}
			},
		},
	}
}

// push a new connection to the end of the chain
func (l *ConnList) Push(c net.Conn) {
	l.chainLock.Lock()
	defer l.chainLock.Unlock()
	item := l.pool.Get().(*ConnSlot) //borrow a slot from pool
	item.c = c
	item.next = nil
	l.cTail.next = item
	l.cTail = item
	l.nCount++
}

// dequeue from the list
func (l *ConnList) Pop() (net.Conn, error) {
	l.chainLock.Lock()
	defer l.chainLock.Unlock()
	if l.cHead.next == nil {
		return nil, errorPoolEmpty
	}
	temp := l.cHead.next
	conn := temp.c
	l.cHead.next = temp.next // concat the chain
	l.pool.Put(temp)         // return the slot to the pool
	l.nCount--
	if l.cHead.next == nil { // last connection released
		l.cTail = l.cHead
	}
	return conn, nil
}

func (l *ConnList) HealthCheck(f func(net.Conn) HealthCondition) {
	l.chainLock.Lock()
	defer l.chainLock.Unlock()
	lastp := l.cHead
	for p := l.cHead.next; p != nil; {
		switch f(p.c) {
		case hcBad:
			{
				lastp.next = p.next
				p.c.Close() // remove bad connections
				l.pool.Put(p)
				l.nCount--
				p = lastp.next
				if p == nil {
					l.cTail = lastp // deal with deleted tail
				}
			}
		case hcCanncel:
			{
				return
			}
		default:
			lastp = p
			p = p.next
		}
	}
}

func (l *ConnList) Count() int {
	return l.nCount
}

type ConnSlot struct {
	c    net.Conn
	next *ConnSlot
}

type ConnMgr struct {
	server        string
	count         int
	nMax          int
	nHeartBeat    int
	cList         *ConnList
	connKickstart chan (bool)
}

func (c *ConnMgr) connKeeper() {
	flag := true
	for {
		time.Sleep(time.Minute * time.Duration(c.nHeartBeat))
		c.cList.HealthCheck(func(conn net.Conn) HealthCondition {
			if pingConn(conn) != nil {
				flag = false
				// log.Println("Found a bad connection, closing it")
				return hcBad
			}
			return hcGood
		})
		if !flag {
			c.connKickstart <- true
		}
	}
}

func (c *ConnMgr) getOneConnection(retry int) (conn net.Conn, e error) {
	for i := 0; i <= retry; i++ {
		if conn, e = net.Dial("tcp", c.server); e == nil {
			return conn, e
		}
	}
	log.Println("Unable to make a new connection with", retry, "retries, reason:", e)
	return nil, e
}

func (c *ConnMgr) Run() {
	for i := 0; i < c.nMax; i++ {
		if conn, e := c.getOneConnection(0); e == nil && conn != nil {
			// log.Println("Initializing pool:", c.cList.Count())
			c.cList.Push(conn)
		}
	}
	go c.connKeeper()
	for {
		<-c.connKickstart
		for {
			// log.Println("Filling with connections.", c.cList.Count(), "in pool")
			if c.cList.Count() < c.nMax {
				if conn, e := c.getOneConnection(0); e == nil && conn != nil {
					c.cList.Push(conn)
				} else {
					time.Sleep(time.Second * 5)
				}
			} else {
				break
			}
		}
	}
}

func (c *ConnMgr) GetConn() net.Conn {
	defer func() { // ask runner to acquire more connections
		select {
		case c.connKickstart <- true:
		default:
		}
	}()
	for {
		if conn, err := c.cList.Pop(); err == nil && conn != nil {
			return &preConnectedConn{
				Conn:        conn,
				bFirstWrite: true,
			}
		} else {
			break
		}
	}
	conn, err := c.getOneConnection(5) // get connection directly without entrying managing phase
	if err == nil {
		return &preConnectedConn{
			Conn:        conn,
			bFirstWrite: true,
		}
	}
	return nil
}

// helper functions
var (
	signalKeepalive = []byte{0}
	signalStart     = []byte{1}
	mtuPool         = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4096) // make it big
		},
	}
)

func pingConn(conn net.Conn) error {
	_, e := conn.Write(signalKeepalive)
	return e
}

type preConnectedConn struct {
	net.Conn
	bFirstWrite bool
}

func (c *preConnectedConn) Write(p []byte) (int, error) {
	if c.bFirstWrite {
		c.bFirstWrite = false
		var morphed []byte
		if len(p) < 4096 {
			morphed = mtuPool.Get().([]byte)
			defer mtuPool.Put(morphed)
		} else {
			morphed = make([]byte, len(signalStart)+len(p))
		}
		copy(morphed[copy(morphed, signalStart):], p)
		n, e := c.Conn.Write(morphed[:len(p)+len(signalStart)])
		if n > 0 {
			n--
		}
		return n, e
	}
	return c.Conn.Write(p)
}
