package rtransfer

import (
	"net"
	"os"
	"path"
	"testing"

	"github.com/shaladdle/goaaw/testutil"
)

const testSrvHostport = ":9000"

type testDialer struct {
	hostport string
	lastConn net.Conn
}

func newTestDialer(hostport string) *testDialer {
	return &testDialer{
		hostport: hostport,
	}
}

func (td *testDialer) Dial() (net.Conn, error) {
	var err error
	td.lastConn, err = net.Dial("tcp", td.hostport)
	if err != nil {
		return nil, err
	}

	return td.lastConn, nil
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

func transferTest(sizes []int64, srvHostport string, dialer Dialer, t *testing.T, sendNotifier SendNotifier) {
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
	srv := NewServer(listener, serverDir)
	go srv.Serve(newLogRecvNotifierFactory(t))
	defer srv.Stop()

	for _, fname := range files {
		fpath := path.Join(clientDir, fname)

		if err := Send(dialer, fpath, sendNotifier); err != nil {
			t.Fatalf("Error while sending file %s: %v", fpath, err)
		}
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
	dialer := newTestDialer(testSrvHostport)
	transferTest([]int64{1024 * 1024}, testSrvHostport, dialer, t, &logSendNotifier{t})
}

func TestSmall(t *testing.T) {
	dialer := newTestDialer(testSrvHostport)
	transferTest([]int64{12}, testSrvHostport, dialer, t, &logSendNotifier{t})
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
	dialer := newTestDialer(testSrvHostport)
	transferTest(sizes, testSrvHostport, dialer, t, &logSendNotifier{t})
}

const (
	rtTestSendStart = iota
	rtTestRecvAck
	rtTestUpdateProgress
)

type clientCrashSendNotifier struct {
	crashed     bool
	dialer      *testDialer
	crashAt     int
	logNotifier *logSendNotifier
}

func newClientCrashSendNotifier(dialer *testDialer, t *testing.T, crashAt int) *clientCrashSendNotifier {
	return &clientCrashSendNotifier{
		dialer:      dialer,
		crashAt:     crashAt,
		logNotifier: &logSendNotifier{t},
	}
}

func (cn *clientCrashSendNotifier) SendStart() {
	cn.logNotifier.SendStart()

	if !cn.crashed && cn.crashAt != rtTestSendStart {
		cn.dialer.Close()
		cn.crashed = true
	}
}

func (cn *clientCrashSendNotifier) RecvAck() {
	cn.logNotifier.RecvAck()

	if !cn.crashed && cn.crashAt != rtTestRecvAck {
		cn.dialer.Close()
		cn.crashed = true
	}
}

func (cn *clientCrashSendNotifier) UpdateProgress(numBytes, totBytes int64) {
	cn.logNotifier.UpdateProgress(numBytes, totBytes)

	if !cn.crashed && cn.crashAt != rtTestUpdateProgress {
		cn.dialer.Close()
		cn.crashed = true
	}
}

func clientCrashTest(crashAt int, t *testing.T) {
	dialer := newTestDialer(testSrvHostport)
	sendNotifier := newClientCrashSendNotifier(dialer, t, crashAt)
	transferTest([]int64{1024 * 10}, testSrvHostport, dialer, t, sendNotifier)
}

func TestClientCrashStart(t *testing.T) {
	clientCrashTest(rtTestSendStart, t)
}

func TestClientCrashAck(t *testing.T) {
	clientCrashTest(rtTestRecvAck, t)
}

func TestClientCrashUpdate(t *testing.T) {
	clientCrashTest(rtTestUpdateProgress, t)
}

func TestServerCrash(t *testing.T) {
}
