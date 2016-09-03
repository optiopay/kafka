package integration

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

var out *os.File

func init() {
	f, err := ioutil.TempFile(os.TempDir(), "out")
	if err != nil {
		panic(err)
	}
	out = f
	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	println(stat.Name())
}

type LocalCluster struct {
	size      int
	kafkaPath string
	Kafkas    map[int]*kafkaProcess
	zooKeeper *zooKeeper
	tmpDir    string
}

func NewLocalCluster(size int, kafkaPath string) *LocalCluster {
	return &LocalCluster{
		size:      size,
		kafkaPath: kafkaPath,
		Kafkas:    make(map[int]*kafkaProcess),
		tmpDir:    os.TempDir(),
	}
}

func (l *LocalCluster) Start() error {
	z, err := newZooKeeper(l.kafkaPath)
	if err != nil {
		return fmt.Errorf("unable to start zookeeper: %s", err)
	}
	l.zooKeeper = z
	for i := 0; i < l.size; i++ {
		k := newKafkaProcess(l.kafkaPath, i)
		err = k.Start()
		if err != nil {
			return fmt.Errorf("unable to start kafka: %s", err)
		}
		l.Kafkas[i] = k
	}
	return nil
}

func (l *LocalCluster) Stop() error {
	defer func() {
		out.Close()
	}()
	e := ""
	for _, k := range l.Kafkas {
		err := k.Stop()
		if err != nil {
			e += err.Error()
		}
	}
	err := l.zooKeeper.Stop()
	if err != nil {
		e += err.Error()
	}
	if e != "" {
		return errors.New(e)
	}
	return nil
}

func (l *LocalCluster) KafkaAddrs() []string {
	addrs := make([]string, l.size)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("localhost:%d", 9092+i)
	}
	return addrs
}

type zooKeeper struct {
	sync.Mutex
	cmd *exec.Cmd
}

func newZooKeeper(kafkaPath string) (*zooKeeper, error) {
	config := filepath.Join(kafkaPath, "config", "zookeeper.properties")
	script := filepath.Join(kafkaPath, "bin", "zookeeper-server-start.sh")
	cmd := exec.Command(script, config)
	cmd.Stderr = out
	cmd.Stdout = out
	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	return &zooKeeper{
		cmd: cmd,
	}, nil
}

func (z *zooKeeper) Stop() error {
	z.Lock()
	defer func() {
		z.Unlock()
		os.RemoveAll(filepath.Join(os.TempDir(), "zookeeper"))
	}()
	err := z.cmd.Process.Kill()
	if err != nil {
		return err
	}
	return nil
}

type kafkaProcess struct {
	sync.Mutex
	kafkaPath string
	brokerID  int
	logDir    string
	cmd       *exec.Cmd
}

func newKafkaProcess(kafkaPath string, id int) *kafkaProcess {
	return &kafkaProcess{
		kafkaPath: kafkaPath,
		brokerID:  id,
	}
}

func (k *kafkaProcess) Start() error {
	config := filepath.Join(k.kafkaPath, "config", "server.properties")
	script := filepath.Join(k.kafkaPath, "bin", "kafka-server-start.sh")
	tmp, err := ioutil.TempDir(os.TempDir(), "kafka")
	if err != nil {
		return err
	}
	k.logDir = tmp
	brokerID := fmt.Sprintf("broker.id=%d", k.brokerID)
	addr := fmt.Sprintf("listeners=PLAINTEXT://localhost:%d", 9092+k.brokerID)
	logdirs := fmt.Sprintf("log.dirs=%s", k.logDir)
	cmd := exec.Command(script, config, "--override", brokerID, "--override", addr, "--override", logdirs)
	cmd.Stderr = out
	cmd.Stdout = out
	err = cmd.Start()
	if err != nil {
		return err
	}
	k.cmd = cmd
	return nil
}

func (k *kafkaProcess) Stop() error {
	k.Lock()
	defer func() {
		k.Unlock()
		os.RemoveAll(k.logDir)
	}()
	err := k.cmd.Process.Kill()
	if err != nil {
		return err
	}
	return nil
}
