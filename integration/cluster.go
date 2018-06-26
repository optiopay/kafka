package integration

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/optiopay/kafka"

	"testing"
)

type KafkaCluster struct {
	// cluster size == number of kafka nodes
	size           int
	kafkaDockerDir string
	verbose        bool

	mu         sync.Mutex
	containers []*Container
}

type Container struct {
	cluster *KafkaCluster
	*docker.Container
}

func NewKafkaCluster(kafkaDockerDir string, size int) *KafkaCluster {
	if size != 1 && size < 4 {
		fmt.Println("WARNING: creating cluster smaller than 4 nodes is not sufficient for all topics")
	}
	return &KafkaCluster{
		kafkaDockerDir: kafkaDockerDir,
		size:           size,
		verbose:        testing.Verbose(),
	}
}

// RunningKafka returns true if container is running kafka node
func (c *Container) RunningKafka() bool {
	return c.Config.Image == "kafkadocker_kafka"
}

// Start starts current container
func (c *Container) Start() error {
	return c.cluster.ContainerStart(c.ID)
}

// Stop stops current container
func (c *Container) Stop() error {
	return c.cluster.ContainerStop(c.ID)
}

func (c *Container) Kill() error {
	return c.cluster.ContainerKill(c.ID)
}

// Start start zookeeper and kafka nodes using docker-compose command. Upon
// successful process spawn, cluster is scaled to required amount of nodes.
func (cluster *KafkaCluster) Start() error {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	// ensure  cluster is not running
	if err := cluster.Stop(); err != nil {
		return fmt.Errorf("cannot ensure stop cluster: %s", err)
	}
	if err := cluster.removeStoppedContainers(); err != nil {
		return fmt.Errorf("cannot cleanup dead containers: %s", err)
	}

	args := []string{"--no-ansi", "up", "-d", "--scale", fmt.Sprintf("kafka=%d", cluster.size)}
	upCmd, _, stderr := cluster.cmd("docker-compose", args...)
	err := upCmd.Run()
	if err != nil {
		return fmt.Errorf("docker-compose error: %s, %s", err, stderr)
	}

	containers, err := cluster.Containers()
	if err != nil {
		_ = cluster.Stop()
		return fmt.Errorf("cannot get containers info: %s", err)
	}
	cluster.containers = containers
	return nil
}

func (cluster *KafkaCluster) WaitUntilReady() error {
	for {
		bconf := kafka.NewBrokerConf("waiter")
		addrs, err := cluster.KafkaAddrs()
		if err != nil {
			return fmt.Errorf("cannot get kafka address: %s", err)
		}

		broker, err := kafka.Dial(addrs, bconf)
		if err != nil {
			return fmt.Errorf("cannot connect to cluster (%q): %s", addrs, err)
		}

		met, err := broker.Metadata()
		if err != nil {
			return fmt.Errorf("Cannot get metadata : %s", err)
		}
		if len(met.Topics) > 0 {
			return nil
		}
		broker.Close()
		time.Sleep(time.Second)
	}
}

// Containers inspect all containers running within cluster and return
// information about them.
func (cluster *KafkaCluster) Containers() ([]*Container, error) {

	psCmd, stdout, stderr := cluster.cmd("docker-compose", "ps", "-q")
	err := psCmd.Run()
	if err != nil {
		return nil, fmt.Errorf("Cannot list processes: %s, %s", err, stderr.String())
	}
	containerIDs := stdout.String()

	var containers []*Container

	endpoint := "unix:///var/run/docker.sock"
	client, err := docker.NewClient(endpoint)
	if err != nil {
		return nil, fmt.Errorf("Cannot open connection to docker %s", err)
	}
	for _, containerID := range strings.Split(strings.TrimSpace(containerIDs), "\n") {
		if containerID == "" {
			continue
		}
		container, err := client.InspectContainer(containerID)
		if err != nil {
			return nil, fmt.Errorf("Cannot inspect docker container %s", err)
		}
		containers = append(containers, &Container{cluster: cluster, Container: container})
	}

	return containers, nil
}

// Stop stop all services running for the cluster by sending SIGINT to
// docker-compose process.
func (cluster *KafkaCluster) Stop() error {
	cmd, _, stderr := cluster.cmd("docker-compose", "stop", "-t", "0")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker-compose stop failed: %s, %s", err, stderr)
	}
	_ = cluster.removeStoppedContainers()
	return nil
}

// KafkaAddrs return list of kafka node addresses as strings, in form
// <host>:<port>
func (cluster *KafkaCluster) KafkaAddrs() ([]string, error) {
	containers, err := cluster.Containers()
	if err != nil {
		return nil, fmt.Errorf("Cannot get containers info: %s", err)
	}
	addrs := make([]string, 0)
	for _, container := range containers {
		if _, ok := container.NetworkSettings.Ports["9092/tcp"]; ok {
			for _, network := range container.NetworkSettings.Networks {
				if network.IPAddress != "" {
					addrs = append(addrs, fmt.Sprintf("%s:%s", network.IPAddress, "9092"))
					break
				}
			}
		}
	}
	return addrs, nil
}
func (cluster *KafkaCluster) ContainerNetworkIP(container Container, network string) (string, error) {
	inspectCmd, stdout, stderr := cluster.cmd("docker", "inspect", ".NetworkSettings.Networks."+network+".IPAddress", container.ID)
	err := inspectCmd.Run()
	if err != nil {
		return "", fmt.Errorf("Cannot inspect %#v: %s, %s", container, err, stderr)
	}
	cleanIP := strings.TrimSpace(stdout.String())
	return cleanIP, nil
}
func (cluster *KafkaCluster) ContainerStop(containerID string) error {
	stopCmd, _, stderr := cluster.cmd("docker", "stop", containerID)
	err := stopCmd.Run()
	if err != nil {
		return fmt.Errorf("cannot stop %q container: %s, %s", containerID, err, stderr)
	}
	return nil
}

func (cluster *KafkaCluster) ContainerKill(containerID string) error {
	killCmd, _, stderr := cluster.cmd("docker", "kill", containerID)
	err := killCmd.Run()
	if err != nil {
		return fmt.Errorf("cannot kill %q container: %s, %s", containerID, err, stderr)
	}
	return nil
}

func (cluster *KafkaCluster) ContainerStart(containerID string) error {
	startCmd, _, stderr := cluster.cmd("docker", "start", containerID)
	err := startCmd.Run()
	if err != nil {
		return fmt.Errorf("cannot start %q container: %s, %s", containerID, err, stderr)
	}
	return nil
}

func (cluster *KafkaCluster) cmd(name string, args ...string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	cmd := exec.Command(name, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = cluster.kafkaDockerDir
	return cmd, &stdout, &stderr
}

func (cluster *KafkaCluster) removeStoppedContainers() error {
	rmCmd, _, stderr := cluster.cmd("docker-compose", "rm", "-f")
	err := rmCmd.Run()
	if err != nil {
		return fmt.Errorf("docker-compose rm error: %s, %s", err, stderr)
	}
	return nil
}
