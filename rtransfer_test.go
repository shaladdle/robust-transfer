package rtransfer

import (
	"net"
	"os"
	"path"
	"testing"

	"github.com/shaladdle/goaaw/testutil"
)

type testDialer struct {
	hostport string
	lastConn net.Conn
}

func NewTestDialer(hostport string) *testDialer {
	return &testDialer{
		hostport: hostport,
	}
}

func (td *testDialer) Dial() (net.Conn, error) {
	conn, err := net.Dial("tcp", td.hostport)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (td *testDialer) Close() {
	td.lastConn.Close()
}

type logSendNotifier struct {
	t *testing.T
}

func (sn *logSendNotifier) SendStart() {
	sn.t.Log("CLI Sending start message")
}

func (sn *logSendNotifier) RecvAck() {
	sn.t.Log("CLI Receiving ack message")
}

func (sn *logSendNotifier) UpdateProgress(numBytes, totBytes int64) {
	sn.t.Logf("CLI Sent %d/%d bytes", numBytes, totBytes)
}

type logRecvNotifier struct {
	t *testing.T
}

func newLogRecvNotifierFactory(t *testing.T) func() RecvNotifier {
	return func() RecvNotifier {
		return &logRecvNotifier{t}
	}
}

func (sn *logRecvNotifier) SendAck() {
	sn.t.Log("SRV Sending ack message")
}

func (sn *logRecvNotifier) RecvStart() {
	sn.t.Log("SRV Receiving start message")
}

func (sn *logRecvNotifier) UpdateProgress(numBytes, totBytes int64) {
	sn.t.Logf("SRV Received %d/%d bytes", numBytes, totBytes)
}

func transferTest(sizes []int64, srvHostport string, t *testing.T) {
	dpath, err := testutil.CreateTestDir()
	if err != nil {
		t.Fatalf("Couldn't create test directory")
	}

	clientDir := path.Join(dpath, "client")
	err = testutil.TryMkdir(clientDir)
	if err != nil {
		t.Fatalf("Couldn't create client test directory")
	}

	serverDir := path.Join(dpath, "server")
	err = testutil.TryMkdir(serverDir)
	if err != nil {
		t.Fatalf("Couldn't create server test directory")
	}

	files := make([]string, len(sizes))
	for i, size := range sizes {
		fname, err := testutil.GenRandName(12)
		if err != nil {
			t.Fatalf("Couldn'generate random name: %s", err)
		}
		files[i] = fname

		err = testutil.GenRandFile(path.Join(clientDir, files[i]), size)
		if err != nil {
			t.Fatalf("Couldn't create random file of size %d: %s", size, err)
		}
	}

	listener, err := net.Listen("tcp", srvHostport)
	if err != nil {
		t.Fatalf("couldn't listen on %s: %s", srvHostport, err)
	}
	defer listener.Close()
	srv := NewServer(listener, serverDir)
	go srv.Serve(newLogRecvNotifierFactory(t))

	for _, fname := range files {
		dialer := NewTestDialer(srvHostport)
		fpath := path.Join(clientDir, fname)

		SendRetry(dialer, fpath, &logSendNotifier{t})
	}

	for _, fname := range files {
		srcHash, err := testutil.HashFile(path.Join(clientDir, fname))
		if err != nil {
			continue
			t.Errorf("Couldn't hash file \"%s\"", fname)
		}

		dstHash, err := testutil.HashFile(path.Join(serverDir, fname))
		if err != nil {
			continue
			t.Errorf("Couldn't hash file \"%s\"", fname)
		}

		if srcHash != dstHash {
			t.Errorf("Hashes don't match. Got %s, wanted %s", dstHash, srcHash)
		}
	}

	if err := os.RemoveAll(dpath); err != nil {
		t.Fatal("Cleanup of temp directory failed:", err)
	}
}

func TestSimple(t *testing.T) {
	transferTest([]int64{1024 * 1024}, ":9000", t)
}

func TestSmall(t *testing.T) {
	transferTest([]int64{12}, ":9000", t)
}

func TestMulti(t *testing.T) {
	const MB = 1024 * 1024
	sizes := []int64{
		MB,
		MB,
		MB,
		5 * MB,
		5 * MB,
	}
	transferTest(sizes, ":9000", t)
}

type clientCrashSendNotifier struct {
	sendStart      chan bool
	recvAck        chan bool
	updateProgress chan bool
}

/*
func TestClientCrash(t *testing.T) {
	sendComplete := make(chan bool)

	fpath := ""

	dialer := NewCrashDialer(srvHostport)

	go func() {
		SendRetry(dialer, srvHostport)
		sendComplete <- true
	}()

	for {
		select {
		case <-sendStart:
			dialer.Close()
			sendStart <- true
		case <-recvAck:
			sendStart <- true
		case <-updateProgress:
			sendStart <- true
		case <-sendComplete:
			sendStart <- true
		}
	}
}
*/

func TestServerCrash(t *testing.T) {
}
