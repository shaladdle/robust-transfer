package rtransfer

import (
	"net"
	"os"
	"path"
	"testing"

	"github.com/shaladdle/goaaw/testutil"
)

const (
	srvHostport = ":9001"
	dmnHostport = ":9000"
)

func daemonTest(sizes []int64, srvHostport string, dialer Dialer, t *testing.T, sendNotifier SendNotifier) {
	dmnErr := make(chan error)
	srvErr := make(chan error)

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
	go func() {
		srvErr <- srv.Serve(newLogRecvNotifierFactory(t))
	}()

	defer func() {
		select {
		case err := <-srvErr:
			t.Fatalf("server returned an error: %v", err)
		default:
			srv.Stop()
		}
	}()

	dmn := NewDaemon(dmnHostport, srvHostport)
	go func() {
		dmnErr <- dmn.Serve()
	}()

	defer func() {
		select {
		case err := <-dmnErr:
			t.Fatalf("daemon returned an error: %v", err)
		default:
			dmn.Stop()
		}
	}()

	for _, fname := range files {
		fpath := path.Join(clientDir, fname)

		if err := SendToDaemon(fpath, dmnHostport); err != nil {
			t.Fatalf("Error while sending file to daemon %s: %v", fpath, err)
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

func TestDaemonSimple(t *testing.T) {
	dialer := newTestDialer(testSrvHostport)
	transferTest([]int64{1024 * 1024}, testSrvHostport, dialer, t, &logSendNotifier{t})
}
