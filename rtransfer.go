package rtransfer

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"time"
)

const maxRetryTime = time.Second * 20

type Dialer interface {
	Dial() (net.Conn, error)
}

const (
	ErrSuccess = iota
	ErrAlreadyExists
	ErrEmptyFilename
	ErrWrongFile
	ErrOpen
)

func strErrMsg(errType int) string {
	switch errType {
	case ErrSuccess:
		return "success"
	case ErrAlreadyExists:
		return "the file already exists"
	case ErrEmptyFilename:
		return "attempt to copy a file with an empty file name"
	case ErrWrongFile:
		return "attempt to copy a different file than the one the server is currently waiting for"
	case ErrOpen:
		return "could not open the file for writing on the server"
	default:
		return "unknown error"
	}
}

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
	Name    string
	SeqNum  int
	Size    int64
	ErrType int
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

func Send(dialer Dialer, fpath string, notifier SendNotifier) {
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

		if err := send(conn, fpath, notifier); err != nil {
			logf("Send error: %v", err)
			cleanup(conn)
			continue
		}

		break
	}
}

func send(conn net.Conn, fpath string, notifier SendNotifier) error {
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

	if ack.ErrType != ErrSuccess {
		return fmt.Errorf(strErrMsg(ack.ErrType))
	}

	f, err := os.Open(fpath)
	if err != nil {
		return err
	}

	numBlocks := getNumBlocks(info.Size())
	seqNum := ack.SeqNum
	for seqNum < numBlocks {
		dataMsg := dataMessage{SeqNum: seqNum, Data: make([]byte, payloadSize)}
		if n, err := f.Read(dataMsg.Data); err != io.EOF && err != nil {
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

func fileExists(fpath string) bool {
	if _, err := os.Stat(fpath); err != nil {
		return false
	}
	return true
}

func (srv *server) recv(conn net.Conn, createNotifier func() RecvNotifier) error {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	sendClientErr := func(errType int, err error) error {
		if err := enc.Encode(ackMessage{ErrType: errType}); err != nil {
			return fmt.Errorf("Error sending client an error message: %v", err)
		}
		return err
	}

	var notifier RecvNotifier
	if createNotifier != nil {
		notifier = createNotifier()
	}

	if createNotifier != nil {
		notifier.RecvStart()
	}

	var startMsg startMessage
	if err := dec.Decode(&startMsg); err != nil {
		return err
	}

	if startMsg.Name == "" {
		return sendClientErr(ErrEmptyFilename,
			fmt.Errorf("Client tried to send a file with no name"))
	} else if srv.name != "" && srv.name != startMsg.Name {
		retErr := fmt.Errorf("Client wants to send %s, but I'm waiting for %s",
			startMsg.Name, srv.name)
		return sendClientErr(ErrWrongFile, retErr)
	}

	fpath := path.Join(srv.archiveDir, startMsg.Name)

	if fileExists(fpath) && srv.name != startMsg.Name {
		return sendClientErr(ErrAlreadyExists,
			fmt.Errorf("Client tried to send a file (%s) that already exists", startMsg.Name))
	}

	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return sendClientErr(ErrOpen, err)
	}
	defer f.Close()

	if srv.name == "" {
		srv.name = startMsg.Name
		srv.size = startMsg.Size
		srv.seqNum = 0
	}
	if createNotifier != nil {
		notifier.SendAck()
	}

	ackMsg := ackMessage{
		Name:    srv.name,
		Size:    srv.size,
		SeqNum:  srv.seqNum,
		ErrType: ErrSuccess,
	}
	if err := enc.Encode(ackMsg); err != nil {
		return err
	}

	numBlocks := getNumBlocks(srv.size)
	for srv.seqNum < numBlocks {
		var dataMsg dataMessage
		if err := dec.Decode(&dataMsg); err != nil {
			return err
		}

		if _, err := f.WriteAt(dataMsg.Data, getFilePos(srv.seqNum)); err != nil {
			return err
		}

		if err := enc.Encode(dataAckMessage{srv.seqNum}); err != nil {
			return err
		}

		srv.seqNum++

		if createNotifier != nil {
			numBytes := getFilePos(srv.seqNum)
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
			logf("recv returned an error: %v", err)
			continue
		}
	}
	return fmt.Errorf("Not implemented")
}
