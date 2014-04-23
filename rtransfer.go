package rtransfer

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"os"
    "path"
)

func init() {
	gob.Register(startMessage{})
	gob.Register(ackMessage{})
	gob.Register(dataMessage{})
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

func Send(conn net.Conn, fpath string) error {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	info, err := os.Stat(fpath)
	if err != nil {
		return err
	}

	startMsg := startMessage{info.Name(), info.Size()}
	if err := enc.Encode(startMsg); err != nil {
		return err
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
	for seqNum := ack.SeqNum; seqNum < numBlocks; seqNum++ {
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
	}

	return nil
}

type Server interface {
    Serve() error
}

type server struct {
    listener net.Listener
    archiveDir string
    name string
    size int64
    seqNum int
}

func NewServer(listener net.Listener, archiveDir string) Server {
    return &server{
        listener: listener,
        archiveDir: archiveDir,
    }
}

func (srv *server) recv(conn net.Conn) error {
    enc := gob.NewEncoder(conn)
    dec := gob.NewDecoder(conn)

    ackMsg := ackMessage{
        Name: srv.name,
        Size: srv.size,
        SeqNum: srv.seqNum,
    }
    if err := enc.Encode(ackMsg); err != nil {
        return err
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
    f, err := os.OpenFile(fpath, os.O_CREATE | os.O_RDWR, 0666)
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
    }

    srv.name = ""
    srv.size = 0
    srv.seqNum = 0

    return nil
}

func (srv *server) Serve() error {
    for {
        conn, err := srv.listener.Accept()
        if err != nil {
            return err
        }

        if err := srv.recv(conn); err != nil {
            return err
        }
    }
	return fmt.Errorf("Not implemented")
}
