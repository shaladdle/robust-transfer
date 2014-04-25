package rtransfer

import (
	"net"
	"time"
)

type Dialer interface {
	Dial() (net.Conn, error)
}

func SendRetry(dialer Dialer, fpath string, notifier SendNotifier) {
	retryTime := time.Millisecond * 200

	cleanup := func(conn net.Conn) {
		c := time.After(retryTime)
		conn.Close()
		retryTime *= 2
		<-c
	}

	for {
		conn, err := dialer.Dial()
		if err != nil {
			cleanup(conn)
			continue
		}

		if err := Send(conn, fpath, notifier); err != nil {
			cleanup(conn)
			continue
		}

		break
	}
}
