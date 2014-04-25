package rtransfer

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"os"
	"path"
)

type SendNotifier interface {
	SendStart()
	RecvAck()
	UpdateProgress(numBytes, totBytes int64)
}

type RecvNotifier interface {
	SendAck()
	RecvStart()
	UpdateProgress(numBytes, totBytes int64)
}

func init() {
	gob.Register(startMessage{})
	gob.Register(ackMessage{})
	gob.Register(dataMessage{})
	gob.Register(dataAckMessage{})
}

const payloadSize = 4096

type startMessage struct {
	Name string
	Size int64
}

type ackMessage struct {
	Name   string
	SeqNum int
	Size   int64
}

type dataMessage struct {
	SeqNum int
	Data   []byte
}

type dataAckMessage struct {
	SeqNum int
}

func getNumBlocks(size int64) int {
	numBlocks := size / payloadSize
	if size%payloadSize != 0 {
		numBlocks++
	}
	return int(numBlocks)
}

func getFilePos(seqNum int) int64 {
	return int64(seqNum) * int64(payloadSize)
}

func Send(conn net.Conn, fpath string, notifier SendNotifier) error {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	info, err := os.Stat(fpath)
	if err != nil {
		return err
	}

	if notifier != nil {
		notifier.SendStart()
	}

	startMsg := startMessage{info.Name(), info.Size()}
	if err := enc.Encode(startMsg); err != nil {
		return err
	}

	if notifier != nil {
		notifier.RecvAck()
	}

	var ack ackMessage
	if err := dec.Decode(&ack); err != nil {
		return err
	}

	if ack.Name != "" && ack.Name != info.Name() {
		return fmt.Errorf(
			"The server is expecting a file named %s, while we are trying to send %s",
			ack.Name, info.Name())
	}

	f, err := os.Open(fpath)
	if err != nil {
		return err
	}

	numBlocks := getNumBlocks(info.Size())
	seqNum := ack.SeqNum
	for seqNum < numBlocks {
		dataMsg := dataMessage{SeqNum: seqNum, Data: make([]byte, payloadSize)}
		if n, err := f.Read(dataMsg.Data[:]); err != io.EOF && err != nil {
			return err
		} else if err == io.EOF && seqNum != numBlocks-1 {
			return fmt.Errorf(
				"Hit end of file at %d, while the last block index expected was %d",
				seqNum, numBlocks-1)
		} else {
			dataMsg.Data = dataMsg.Data[:n]
		}

		if err := enc.Encode(dataMsg); err != nil {
			return err
		}

		var dataAckMsg dataAckMessage
		if err := dec.Decode(&dataAckMsg); err != nil {
			return err
		}

		if dataAckMsg.SeqNum != seqNum {
			return fmt.Errorf(
				"Server acked a payload with a different sequence number, got %d, want %d",
				dataAckMsg.SeqNum, seqNum)
		}

		seqNum++

		if notifier != nil {
			numBytes := getFilePos(seqNum)
			if numBytes > info.Size() {
				numBytes = info.Size()
			}
			notifier.UpdateProgress(numBytes, info.Size())
		}
	}

	return nil
}

type Server interface {
	Serve(func() RecvNotifier) error
}

type server struct {
	listener   net.Listener
	archiveDir string
	name       string
	size       int64
	seqNum     int
}

func NewServer(listener net.Listener, archiveDir string) Server {
	return &server{
		listener:   listener,
		archiveDir: archiveDir,
	}
}

func (srv *server) recv(conn net.Conn, createNotifier func() RecvNotifier) error {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	var notifier RecvNotifier
	if createNotifier != nil {
		notifier = createNotifier()
		notifier.SendAck()
	}

	ackMsg := ackMessage{
		Name:   srv.name,
		Size:   srv.size,
		SeqNum: srv.seqNum,
	}
	if err := enc.Encode(ackMsg); err != nil {
		return err
	}

	if createNotifier != nil {
		notifier.RecvStart()
	}

	var startMsg startMessage
	if err := dec.Decode(&startMsg); err != nil {
		return err
	}
	if srv.name == "" {
		srv.name = startMsg.Name
		srv.size = startMsg.Size
		srv.seqNum = 0
	}
	if srv.name != "" && startMsg.Name != srv.name {
		return fmt.Errorf("Client tried to send a file I am not waiting for")
	}

	fpath := path.Join(srv.archiveDir, srv.name)
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		srv.seqNum = 0
		return err
	}

	numBlocks := getNumBlocks(srv.size)
	seqNum := srv.seqNum
	for seqNum < numBlocks {
		var dataMsg dataMessage
		if err := dec.Decode(&dataMsg); err != nil {
			return err
		}

		if _, err := f.WriteAt(dataMsg.Data[:], getFilePos(seqNum)); err != nil {
			return err
		}

		if err := enc.Encode(dataAckMessage{seqNum}); err != nil {
			return err
		}

		seqNum++

		if createNotifier != nil {
			numBytes := getFilePos(seqNum)
			if numBytes > srv.size {
				numBytes = srv.size
			}
			notifier.UpdateProgress(numBytes, srv.size)
		}
	}

	srv.name = ""
	srv.size = 0
	srv.seqNum = 0

	return nil
}

func (srv *server) Serve(createNotifier func() RecvNotifier) error {
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			return err
		}

		if err := srv.recv(conn, createNotifier); err != nil {
			continue
		}
	}
	return fmt.Errorf("Not implemented")
}
