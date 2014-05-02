package rtransfer

import (
	"container/list"
	"encoding/gob"
	"net"
)

type simpleDialer string

func (s simpleDialer) Dial() (net.Conn, error) {
	return net.Dial("tcp", string(s))
}

type Daemon interface {
	Serve() error
	Stop()
}

type daemon struct {
	dmnHostport string
	srvHostport string
	newFiles    chan string
	stop        chan bool
	stopped     bool
	listener    net.Listener
}

func NewDaemon(dmnHostport, srvHostport string) Daemon {
	return &daemon{
		dmnHostport: dmnHostport,
		srvHostport: srvHostport,
		newFiles:    make(chan string),
		stop:        make(chan bool),
	}
}

func (d *daemon) handleConn(conn net.Conn) error {
	defer conn.Close()

	dec := gob.NewDecoder(conn)

	var fpath string
	if err := dec.Decode(&fpath); err != nil {
		return err
	}

	logf("Received request to send file %s", fpath)

	d.newFiles <- fpath

	return nil
}

func (d *daemon) Serve() error {
	go d.director()

	var err error
	d.listener, err = net.Listen("tcp", d.dmnHostport)
	if err != nil {
		return err
	}

	for {
		conn, err := d.listener.Accept()
		if err != nil {
			return err
		}

		if err := d.handleConn(conn); err != nil {
			logf("error handling connection: %v", err)
		}
	}

	return nil
}

func (d *daemon) director() {
	queue := list.New()
	done := make(chan error)
	dialer := simpleDialer(d.srvHostport)

	send := func(fpath string) {
		logf("Sending file %s", fpath)
		done <- Send(dialer, fpath, nil)
	}

Loop:
	for {
		select {
		case <-d.stop:
			break Loop
		case fpath := <-d.newFiles:
			queue.PushBack(fpath)

			if queue.Len() == 1 {
				go send(fpath)
			}
		case err := <-done:
			oldFpath := queue.Front().Value.(string)
			if err != nil {
				logf("An error occurred sending file %s: %v", oldFpath, err)
				// We might want to communicate this failure to the user
			}

			queue.Remove(queue.Front())

			if queue.Len() > 0 {
				go send(queue.Front().Value.(string))
			}
		}
	}
}

func (d *daemon) Stop() {
	if !d.stopped {
		d.listener.Close()
		d.stop <- true
		d.stopped = true
	}
}

func SendToDaemon(fpath, hostport string) error {
	conn, err := net.Dial("tcp", hostport)
	if err != nil {
		return err
	}

	enc := gob.NewEncoder(conn)
	if err := enc.Encode(fpath); err != nil {
		return err
	}
	return nil
}
