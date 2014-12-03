package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"github.com/ajackson/metron/contrail"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

var appId string

const (
	maxMessageLength = 10 * 1024
)

func init() {
	flag.StringVar(&appId, "appId", "", "Application ID to add to all messages, omit if neccessary")
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		return
	}
	Exec(flag.Arg(0), flag.Args()[1:])
}

func Exec(cmdString string, args []string) error {
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()
	defer stdoutReader.Close()
	defer stderrReader.Close()

	cmd := exec.Command(cmdString, args...)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	os.MkdirAll(filepath.Join(os.TempDir(), "iomagic", cmdString), 0700)
	inSock := filepath.Join(os.TempDir(), "iomagic", cmdString, time.Now().Format(time.RFC3339Nano))
	conn, err := net.ListenPacket("unixgram", inSock)
	if err != nil {
		panic(err)
	}
	defer os.Remove(inSock)
	defer conn.Close()

	statusAddr, _ := net.ResolveUnixAddr("unixgram", "/tmp/metron_status.sock")
	statusEncoder := gob.NewEncoder(&PacketConnWriter{conn: conn, addr: statusAddr})

	go listenToStdOut(cmdString, conn, stdoutReader)

	go listenToStdErr(cmdString, conn, stderrReader)

	println("Running Command", cmdString)

	err = cmd.Start()
	ps := contrail.NewProcessStatusStart(appId, cmdString, cmd.Process)
	statusEncoder.Encode(ps)
	if (err != nil) {
		pc := contrail.NewProcessStatusComplete(appId, cmdString, cmd.ProcessState)
		statusEncoder.Encode(pc)
		return err
	}
	err = cmd.Wait()
	pc := contrail.NewProcessStatusComplete(appId, cmdString, cmd.ProcessState)
	statusEncoder.Encode(pc)
	return err
}

func processConnection(metronWriter io.Writer, processOutput io.Reader, logMessage contrail.LogLine) {
	encoder := gob.NewEncoder(metronWriter)
	err := bufio.ErrTooLong
	for err == bufio.ErrTooLong {
		scanner := bufio.NewScanner(processOutput)
		for scanner.Scan() {
			message := scanner.Bytes()
			messageLength := len(message)
			if messageLength == 0 {
				continue
			}
			if messageLength > maxMessageLength {
				message = message[:maxMessageLength]
			}
			logMessage.Message = message
			logMessage.Timestamp = time.Now()
			logMessage.Sequence++
			encoder.Encode(logMessage)
		}
		err = scanner.Err()
	}
}

func listenToStdOut(cmdString string, conn net.PacketConn, processReader io.Reader) {
	stdOutAddr, _ := net.ResolveUnixAddr("unixgram", "/tmp/metron_out.sock")
	logMessage := contrail.LogLine{AppId: appId, Command: cmdString, OutputType: contrail.Stdout}
	processConnection(&PacketConnWriter{conn: conn, addr: stdOutAddr}, processReader, logMessage)
}

func listenToStdErr(cmdString string, conn net.PacketConn, processReader io.Reader) {
	stdErrAddr, _ := net.ResolveUnixAddr("unixgram", "/tmp/metron_err.sock")
	logMessage := contrail.LogLine{AppId: appId, Command: cmdString, OutputType: contrail.Stderr}
	processConnection(&PacketConnWriter{conn: conn, addr: stdErrAddr}, processReader, logMessage)
}

type PacketConnWriter struct {
	conn net.PacketConn
	addr net.Addr
}

func (w *PacketConnWriter) Write(p []byte) (n int, err error) {
	return w.conn.WriteTo(p, w.addr)
}
