package rtransfer

import (
	"net"
	"time"
)

const maxRetryTime = time.Second * 20

type Dialer interface {
	Dial() (net.Conn, error)
}

func SendRetry(dialer Dialer, fpath string, notifier SendNotifier) {
	retryTime := time.Millisecond * 200

	cleanup := func(conn net.Conn) {
		logf("retrying after %v", retryTime)
		c := time.After(retryTime)
		conn.Close()
		if retryTime < maxRetryTime {
			retryTime *= 2
		}
		<-c
	}

	for {
		conn, err := dialer.Dial()
		if err != nil {
			logf("Dial error: %v", err)
			cleanup(conn)
			continue
		}

		if err := Send(conn, fpath, notifier); err != nil {
			logf("Send error: %v", err)
			cleanup(conn)
			continue
		}

		break
	}
}
