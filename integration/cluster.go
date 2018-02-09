package integration

import (
	"fmt"
	"github.com/fsouza/go-dockerclient"
	"os"
	"os/exec"
	"strings"
	"sync"

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
		fmt.Fprintln(os.Stderr,
			"WARNING: creating cluster smaller than 4 nodes is not sufficient for all topics")
	}
	return &KafkaCluster{
		kafkaDockerDir: kafkaDockerDir,
		size:           size,
		verbose:        testing.Verbose(),
	}
}

// RunningKafka returns true if container is running kafka node
func (c *Container) RunningKafka() bool {
	return c.Args[1] == "start-kafka.sh"
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

	args := []string{"up", "-d", "--scale", fmt.Sprintf("kafka=%d", cluster.size)}
	upCmd := cluster.cmd("docker-compose", args...)
	if err := upCmd.Run(); err != nil {
		return fmt.Errorf("docker-compose error: %s", err)
	}

	containers, err := cluster.Containers()
	if err != nil {
		_ = cluster.Stop()
		return fmt.Errorf("cannot get containers info: %s", err)
	}
	cluster.containers = containers
	return nil
}

// Containers inspect all containers running within cluster and return
// information about them.
func (cluster *KafkaCluster) Containers() ([]*Container, error) {
	psCmd := cluster.cmd("docker-compose", "ps", "-q")
	out, err := psCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("cannot list processes: %s", err)
	}
	dockerIDs := string(out)
	fmt.Println(dockerIDs)
	endpoint := "unix:///var/run/docker.sock"
	client, err := docker.NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	var containers []*Container
	for _, dockerID := range strings.Split(dockerIDs, "\n") {
		dockerContainer, err := client.InspectContainer(dockerID)
		if err != nil {
			fmt.Println("Failure to parse")
			return nil, err
		}
		container := &Container{cluster, dockerContainer}
		containers = append(containers, container)

	}

	return containers, nil
}

// Stop stop all services running for the cluster by sending SIGINT to
// docker-compose process.
func (cluster *KafkaCluster) Stop() error {
	cmd := cluster.cmd("docker-compose", "stop", "-t", "0")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker-compose stop failed: %s", err)
	}
	_ = cluster.removeStoppedContainers()
	return nil
}

// KafkaAddrs return list of kafka node addresses as strings, in form
// <host>:<port>
func (cluster *KafkaCluster) KafkaAddrs() ([]string, error) {
	containers, err := cluster.Containers()
	if err != nil {
		return nil, fmt.Errorf("cannot get containers info: %s", err)
	}
	addrs := make([]string, 0)
	for _, container := range containers {
		ports, ok := container.NetworkSettings.Ports["9092/tcp"]
		if !ok || len(ports) == 0 {
			continue
		}
		ip := ports[0].HostIP
		if ip == "0.0.0.0" {
			// find the first ip of the container
			//fmt.Printf("%s\n", container.NetworkSettings)
			if container.NetworkSettings.IPAddress != "" {
				ip = container.NetworkSettings.IPAddress
			} else {
				for _, v := range container.NetworkSettings.Networks {
					ip = v.IPAddress
					break
				}
				if ip == "" {
					return nil, fmt.Errorf("Coulnd't find an ip for container %s", container.ID)
				}
			}

		}
		addrs = append(addrs, fmt.Sprintf("%s:%s", ip, ports[0].HostPort))
	}
	return addrs, nil
}

func (cluster *KafkaCluster) ContainerStop(containerID string) error {
	stopCmd := cluster.cmd("docker", "stop", containerID)
	if err := stopCmd.Run(); err != nil {
		return fmt.Errorf("cannot stop %q container: %s", containerID, err)
	}
	return nil
}

func (cluster *KafkaCluster) ContainerKill(containerID string) error {
	killCmd := cluster.cmd("docker", "kill", containerID)
	if err := killCmd.Run(); err != nil {
		return fmt.Errorf("cannot kill %q container: %s", containerID, err)
	}
	return nil
}

func (cluster *KafkaCluster) ContainerStart(containerID string) error {
	startCmd := cluster.cmd("docker", "start", containerID)
	if err := startCmd.Run(); err != nil {
		return fmt.Errorf("cannot start %q container: %s", containerID, err)
	}
	return nil
}

func (cluster *KafkaCluster) cmd(name string, args ...string) *exec.Cmd {
	c := exec.Command(name, args...)
	c.Dir = cluster.kafkaDockerDir
	return c
}

func (cluster *KafkaCluster) removeStoppedContainers() error {
	rmCmd := cluster.cmd("docker-compose", "rm", "-f")
	if err := rmCmd.Run(); err != nil {
		return fmt.Errorf("docker-compose rm error: %s", err)
	}
	return nil
}
