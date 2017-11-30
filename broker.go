package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/optiopay/kafka/proto"
)

const (
	// StartOffsetNewest configures the consumer to fetch messages produced
	// after creating the consumer.
	StartOffsetNewest = -1

	// StartOffsetOldest configures the consumer to fetch starting from the
	// oldest message available.
	StartOffsetOldest = -2
)

var (
	// ErrNoData is returned by consumers on Fetch when the retry limit is set
	// and exceeded.
	ErrNoData = errors.New("no data")

	// Make sure interfaces are implemented
	_ Client            = &Broker{}
	_ Consumer          = &consumer{}
	_ Producer          = &producer{}
	_ OffsetCoordinator = &offsetCoordinator{}
	_ Admin             = &admin{}
)

// Client is the interface implemented by Broker.
type Client interface {
	Producer(conf ProducerConf) Producer
	Consumer(ctx context.Context, conf ConsumerConf) (Consumer, error)
	OffsetCoordinator(ctx context.Context, conf OffsetCoordinatorConf) (OffsetCoordinator, error)
	Admin(ctx context.Context, conf AdminConf) (Admin, error)
	OffsetEarliest(ctx context.Context, topic string, partition int32) (offset int64, err error)
	OffsetLatest(ctx context.Context, topic string, partition int32) (offset int64, err error)
	Close()
}

// Consumer is the interface that wraps the Consume method.
//
// Consume reads a message from a consumer, returning an error when
// encountered.
type Consumer interface {
	Consume(ctx context.Context) (*proto.Message, error)
}

// BatchConsumer is the interface that wraps the ConsumeBatch method.
//
// ConsumeBatch reads a batch of messages from a consumer, returning an error
// when encountered.
type BatchConsumer interface {
	ConsumeBatch(ctx context.Context) ([]*proto.Message, error)
}

// Producer is the interface that wraps the Produce method.
//
// Produce writes the messages to the given topic and partition.
// It returns the offset of the first message and any error encountered.
// The offset of each message is also updated accordingly.
type Producer interface {
	Produce(ctx context.Context, topic string, partition int32, messages ...*proto.Message) (offset int64, err error)
}

// OffsetCoordinator is the interface which wraps the Commit and Offset methods.
type OffsetCoordinator interface {
	Commit(ctx context.Context, topic string, partition int32, offset int64) error
	Offset(ctx context.Context, topic string, partition int32) (offset int64, metadata string, err error)
}

// Admin write the admin-client methods.
type Admin interface {
	DeleteTopics(ctx context.Context, topics []string, timeout int32) ([]proto.TopicErrorCode, error)
	DescribeConfigs(ctx context.Context, configs ...proto.ConfigResource) ([]proto.ConfigResourceEntry, error)
}

type topicPartition struct {
	topic     string
	partition int32
}

func (tp topicPartition) String() string {
	return fmt.Sprintf("%s:%d", tp.topic, tp.partition)
}

type clusterMetadata struct {
	created      time.Time
	nodes        map[int32]string         // node ID to address
	endpoints    map[topicPartition]int32 // partition to leader node ID
	partitions   map[string]int32         // topic to number of partitions
	controllerID *int32                   // node ID of controller
	clusterID    string                   // ID of the cluster
}

// BrokerConf represents the configuration of a broker.
type BrokerConf struct {
	// Kafka client ID.
	ClientID string

	// LeaderRetryLimit limits the number of connection attempts to a single
	// node before failing. Use LeaderRetryWait to control the wait time
	// between retries.
	//
	// Defaults to 10.
	LeaderRetryLimit int

	// LeaderRetryWait sets a limit to the waiting time when trying to connect
	// to a single node after failure.
	//
	// Defaults to 500ms.
	//
	// Timeout on a connection is controlled by the DialTimeout setting.
	LeaderRetryWait time.Duration

	// AllowTopicCreation enables a last-ditch "send produce request" which
	// happens if we do not know about a topic. This enables topic creation
	// if your Kafka cluster is configured to allow it.
	//
	// Defaults to False.
	AllowTopicCreation bool

	// If set, a TLS connection is created.
	DialTLS bool

	// Configuration used for TLS connections
	TLSConfig *tls.Config

	// Any new connection dial timeout.
	//
	// Default is 10 seconds.
	DialTimeout time.Duration

	// DialRetryLimit limits the number of connection attempts to every node in
	// cluster before failing. Use DialRetryWait to control the wait time
	// between retries.
	//
	// Defaults to 10.
	DialRetryLimit int

	// DialRetryWait sets a limit to the waiting time when trying to establish
	// broker connection to single node to fetch cluster metadata.
	//
	// Defaults to 500ms.
	DialRetryWait time.Duration

	// ReadTimeout is TCP read timeout
	//
	// Default is 30 seconds
	ReadTimeout time.Duration

	// RetryErrLimit limits the number of retry attempts when an error is
	// encountered.
	//
	// Default is 10.
	RetryErrLimit int

	// RetryErrWait controls the wait duration between retries after failed
	// fetch request.
	//
	// Default is 500ms.
	RetryErrWait time.Duration

	// Maximum time a Metadata request is allowed to take.
	// Default is 10sec
	MetadataTimeout time.Duration

	// Maximum time a Offset request is allowed to take.
	// Default is 10sec
	OffsetTimeout time.Duration

	// Maximum time a ConsumerMetadata request is allowed to take.
	ConsumerMetadataTimeout time.Duration

	// DEPRECATED 2015-07-10 - use Logger instead
	//
	// TODO(husio) remove
	//
	// Logger used by the broker.
	Log interface {
		Print(...interface{})
		Printf(string, ...interface{})
	}

	// Logger is general logging interface that can be provided by popular
	// logging frameworks. Used to notify and as replacement for stdlib `log`
	// package.
	Logger Logger
}

// NewBrokerConf returns the default broker configuration.
func NewBrokerConf(clientID string) BrokerConf {
	return BrokerConf{
		ClientID:                clientID,
		DialTimeout:             10 * time.Second,
		DialRetryLimit:          10,
		DialRetryWait:           500 * time.Millisecond,
		AllowTopicCreation:      false,
		LeaderRetryLimit:        10,
		LeaderRetryWait:         500 * time.Millisecond,
		RetryErrLimit:           10,
		RetryErrWait:            time.Millisecond * 500,
		ReadTimeout:             30 * time.Second,
		MetadataTimeout:         10 * time.Second,
		OffsetTimeout:           10 * time.Second,
		ConsumerMetadataTimeout: 10 * time.Second,
		Logger:                  &nullLogger{},
	}
}

// Broker is an abstract connection to kafka cluster, managing connections to
// all kafka nodes.
type Broker struct {
	conf BrokerConf

	mu            sync.Mutex
	metadata      clusterMetadata
	conns         map[int32]*connection
	nodeAddresses []string
}

// Dial connects to any node from a given list of kafka addresses and after
// successful metadata fetch, returns broker.
//
// The returned broker is not initially connected to any kafka node.
func Dial(ctx context.Context, nodeAddresses []string, conf BrokerConf) (*Broker, error) {
	if len(nodeAddresses) == 0 {
		return nil, errors.New("no addresses provided")
	}

	broker := &Broker{
		conf:          conf,
		conns:         make(map[int32]*connection),
		nodeAddresses: nodeAddresses,
	}

	logger := getLogFromContext(ctx, conf.Logger)
	for i := 0; i < conf.DialRetryLimit; i++ {
		if i > 0 {
			logger.Debug("cannot fetch metadata from any connection",
				"retry", i,
				"sleep", conf.DialRetryWait)
			select {
			case <-time.After(conf.DialRetryWait):
				// continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if err := broker.refreshMetadata(ctx, proto.MetadataV0); err == nil {
			return broker, nil
		} else if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("cannot connect")
}

func (b *Broker) getInitialAddresses() []string {
	dest := make([]string, len(b.nodeAddresses))
	perm := rand.Perm(len(b.nodeAddresses))
	for i, v := range perm {
		dest[v] = b.nodeAddresses[i]
	}
	return dest
}

// Close closes the broker and all active kafka nodes connections.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for nodeID, conn := range b.conns {
		if err := conn.Close(); err != nil {
			b.conf.Logger.Info("cannot close node connection",
				"nodeID", nodeID,
				"error", err)
		}
	}
}

// Metadata requests metadata information from any node.
func (b *Broker) Metadata(ctx context.Context) (*proto.MetadataResp, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fetchMetadata(ctx, proto.MetadataV0)
}

// refreshMetadata is requesting metadata information from any node and refresh
// internal cached representation.
// Because it's changing internal state, this method requires lock protection,
// but it does not acquire nor release lock itself.
func (b *Broker) refreshMetadata(ctx context.Context, metadataVersion int16) error {
	meta, err := b.fetchMetadata(ctx, metadataVersion)
	if err == nil {
		b.cacheMetadata(ctx, meta)
	}
	return err
}

// muRefreshMetadata calls refreshMetadata, but protects it with broker's lock.
func (b *Broker) muRefreshMetadata(ctx context.Context, metadataVersion int16) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.refreshMetadata(ctx, metadataVersion); err != nil {
		return err
	}
	return nil
}

// fetchMetadata is requesting metadata information from any node and return
// protocol response if successful
//
// If "topics" are specified, only fetch metadata for those topics (can be
// used to create a topic)
//
// Because it's using metadata information to find node connections it's not
// thread safe and using it require locking.
func (b *Broker) fetchMetadata(ctx context.Context, metadataVersion int16, topics ...string) (*proto.MetadataResp, error) {
	checkednodes := make(map[int32]bool)

	// try all existing connections first
	logger := getLogFromContext(ctx, b.conf.Logger)
	for nodeID, conn := range b.conns {
		checkednodes[nodeID] = true
		resp, err := conn.Metadata(ctx, b.conf.MetadataTimeout, &proto.MetadataReq{
			Version:  metadataVersion,
			ClientID: b.conf.ClientID,
			Topics:   topics,
		})
		if isCloseDeadConnectionNeeded(err) {
			// Note: Mutex is already locked
			b.closeDeadConnection(ctx, conn, false)
		}
		if isCanceled(err) {
			return nil, err
		} else if err != nil {
			logger.Debug("cannot fetch metadata from connected node",
				"nodeID", nodeID,
				"error", err)
			continue
		}
		return resp, nil
	}

	// try all nodes that we know of that we're not connected to
	{
		responses := make(chan *proto.MetadataResp, len(b.metadata.nodes))
		wg := sync.WaitGroup{}
		for nodeID, addr := range b.metadata.nodes {
			wg.Add(1)
			go func(nodeID int32, addr string) {
				defer wg.Done()
				if _, ok := checkednodes[nodeID]; ok {
					return
				}
				conn, err := b.newConnection(ctx, addr)
				if isCanceled(err) {
					return
				} else if err != nil {
					logger.Debug("cannot connect",
						"address", addr,
						"error", err)
					return
				}
				resp, err := conn.Metadata(ctx, b.conf.MetadataTimeout, &proto.MetadataReq{
					Version:  metadataVersion,
					ClientID: b.conf.ClientID,
					Topics:   topics,
				})

				// we had no active connection to this node, so most likely we don't need it
				conn.Close()

				if isCanceled(err) {
					return
				} else if err != nil {
					logger.Debug("cannot fetch metadata from newly connected node",
						"nodeID", nodeID,
						"error", err)
					return
				}
				responses <- resp
			}(nodeID, addr)
		}
		// Wait for cleaining up channels in another goroutine
		go func() {
			wg.Wait()
			close(responses)
		}()

		// Wait for first response
		select {
		case r, ok := <-responses:
			if ok {
				return r, nil
			}
			// Continue with initial addresses
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	{
		addresses := b.getInitialAddresses()
		responses := make(chan *proto.MetadataResp, len(addresses))
		wg := sync.WaitGroup{}
		for _, addr := range addresses {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				conn, err := b.newConnection(ctx, addr)
				if isCanceled(err) {
					return
				} else if err != nil {
					logger.Debug("cannot connect to seed node",
						"address", addr,
						"error", err)
					return
				}
				resp, err := conn.Metadata(ctx, b.conf.MetadataTimeout, &proto.MetadataReq{
					Version:  metadataVersion,
					ClientID: b.conf.ClientID,
					Topics:   topics,
				})
				conn.Close()
				if isCanceled(err) {
					return
				} else if err == nil {
					responses <- resp
				}
				logger.Debug("cannot fetch metadata",
					"address", addr,
					"error", err)
			}(addr)
		}

		// Wait for cleaining up channels in another goroutine
		go func() {
			wg.Wait()
			close(responses)
		}()

		// Wait for first response
		select {
		case r, ok := <-responses:
			if ok {
				return r, nil
			}
			return nil, errors.New("cannot fetch metadata. No topics created?")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// cacheMetadata creates new internal metadata representation using data from
// given response. It's call has to be protected with lock.
//
// Do not call with partial metadata response, this assumes we have the full
// set of metadata in the response
func (b *Broker) cacheMetadata(ctx context.Context, resp *proto.MetadataResp) {
	logger := getLogFromContext(ctx, b.conf.Logger)
	if !b.metadata.created.IsZero() {
		logger.Debug("rewriting old metadata",
			"age", time.Now().Sub(b.metadata.created))
	}
	b.metadata = clusterMetadata{
		created:      time.Now(),
		nodes:        make(map[int32]string),
		endpoints:    make(map[topicPartition]int32),
		partitions:   make(map[string]int32),
		controllerID: nil,
		clusterID:    "",
	}
	if resp.HasControllerID() {
		id := resp.ControllerID
		b.metadata.controllerID = &id
	} else {
		b.metadata.controllerID = nil
	}
	if resp.HasClusterID() {
		b.metadata.clusterID = resp.ClusterID
	}
	var debugmsg []interface{}
	for _, node := range resp.Brokers {
		addr := net.JoinHostPort(node.Host, strconv.Itoa(int(node.Port)))
		b.metadata.nodes[node.NodeID] = addr
		debugmsg = append(debugmsg, node.NodeID, addr)
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			dest := topicPartition{topic.Name, part.ID}
			b.metadata.endpoints[dest] = part.Leader
			debugmsg = append(debugmsg, dest, part.Leader)
		}
		b.metadata.partitions[topic.Name] = int32(len(topic.Partitions))
	}
	logger.Debug("new metadata cached", debugmsg...)
}

// PartitionCount returns how many partitions a given topic has. If a topic
// is not known, 0 and an error are returned.
func (b *Broker) PartitionCount(topic string) (int32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	count, ok := b.metadata.partitions[topic]
	if ok {
		return count, nil
	}

	return 0, fmt.Errorf("topic %s not found in metadata", topic)
}

// newConnection creates a connection to the broker at the given address (host:port).
func (b *Broker) newConnection(ctx context.Context, addr string) (*connection, error) {
	if b.conf.DialTLS {
		conn, err := newTLSConnection(ctx, addr, b.conf.TLSConfig, b.conf.DialTimeout, b.conf.ReadTimeout)
		if err != nil {
			return nil, err
		}
		return conn, err
	}
	conn, err := newTCPConnection(ctx, addr, b.conf.DialTimeout, b.conf.ReadTimeout)
	if err != nil {
		return nil, err
	}
	return conn, err
}

// muLeaderConnection returns connection to leader for given partition. If
// connection does not exist, broker will try to connect first and add store
// connection for any further use.
//
// Failed connection retry is controlled by broker configuration.
//
// If broker is configured to allow topic creation, then if we don't find
// the leader we will return a random broker. The broker will error if we end
// up producing to it incorrectly (i.e., our metadata happened to be out of
// date).
func (b *Broker) muLeaderConnection(ctx context.Context, topic string, partition int32) (*connection, error) {
	logger := getLogFromContext(ctx, b.conf.Logger)
	tp := topicPartition{topic, partition}

	b.mu.Lock()
	defer b.mu.Unlock()

	lastErr := error(proto.ErrBrokerNotAvailable)
	for retry := 0; retry < b.conf.LeaderRetryLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return nil, err
		}

		if retry != 0 {
			func() {
				// Note Unlock, Lock on exit
				b.mu.Unlock()
				defer b.mu.Lock()
				logger.Debug("cannot get leader connection",
					"topic", topic,
					"partition", partition,
					"retry", retry,
					"sleep", b.conf.LeaderRetryWait.String())
				select {
				case <-time.After(b.conf.LeaderRetryWait):
					// continue
				case <-ctx.Done():
					// context canceled
				}
			}()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}

		nodeID, ok := b.metadata.endpoints[tp]
		if !ok {
			if err := b.refreshMetadata(ctx, proto.MetadataV0); isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Info("cannot get leader connection: cannot refresh metadata",
					"error", err)
				lastErr = err
				continue
			}
			nodeID, ok = b.metadata.endpoints[tp]
			if !ok {
				lastErr = proto.ErrUnknownTopicOrPartition
				// If we allow topic creation, now is the point where it is likely that this
				// is a brand new topic, so try to get metadata on it (which will trigger
				// the creation process)
				if b.conf.AllowTopicCreation {
					if _, err := b.fetchMetadata(ctx, proto.MetadataV0, topic); err != nil {
						logger.Info("failed to fetch metadata for new topic",
							"topic", topic,
							"error", err)
					}
				} else {
					logger.Info("cannot get leader connection: unknown topic or partition",
						"topic", topic,
						"partition", partition,
						"endpoint", tp)
				}
				continue
			}
		}

		if conn, ok := b.conns[nodeID]; ok {
			return conn, nil
		}
		addr, ok := b.metadata.nodes[nodeID]
		if !ok {
			logger.Info("cannot get leader connection: no information about node",
				"nodeID", nodeID)
			lastErr = proto.ErrBrokerNotAvailable
			delete(b.metadata.endpoints, tp)
			continue
		}
		if conn, err := b.newConnection(ctx, addr); isCanceled(err) {
			return nil, err
		} else if err != nil {
			logger.Info("cannot get leader connection: cannot connect to node",
				"address", addr,
				"error", err)
			delete(b.metadata.endpoints, tp)
			continue
		} else {
			b.conns[nodeID] = conn
			return conn, nil
		}
	}
	return nil, lastErr
}

// coordinatorConnection returns connection to offset coordinator for given group.
//
// Failed connection retry is controlled by broker configuration.
func (b *Broker) muCoordinatorConnection(ctx context.Context, consumerGroup string) (*connection, error) {
	logger := getLogFromContext(ctx, b.conf.Logger)
	b.mu.Lock()
	defer b.mu.Unlock()

	lastErr := error(proto.ErrNoCoordinator)
	for retry := 0; retry < b.conf.LeaderRetryLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return nil, err
		}

		if retry != 0 {
			func() {
				b.mu.Unlock()
				defer b.mu.Lock()
				select {
				case <-time.After(b.conf.LeaderRetryWait):
					// continue
				case <-ctx.Done():
					// context canceled
				}
			}()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}

		// first try all already existing connections
		for nid, conn := range b.conns {
			resp, err := conn.ConsumerMetadata(ctx, b.conf.ConsumerMetadataTimeout, &proto.ConsumerMetadataReq{
				ClientID:      b.conf.ClientID,
				ConsumerGroup: consumerGroup,
			})
			if isCloseDeadConnectionNeeded(err) {
				// Note: We're already locked
				logger.Info("Closing dead connection",
					"nodeID", nid)
				b.closeDeadConnection(ctx, conn, false)
			}
			if isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Debug("cannot fetch coordinator metadata",
					"consumGrp", consumerGroup,
					"error", err)
				lastErr = err
				continue
			}
			if resp.Err != nil {
				logger.Debug("coordinator metadata response error",
					"consumGrp", consumerGroup,
					"error", resp.Err)
				lastErr = err
				continue
			}

			addr := net.JoinHostPort(resp.CoordinatorHost, strconv.Itoa(int(resp.CoordinatorPort)))
			conn, err := b.newConnection(ctx, addr)
			if isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Debug("cannot connect to node",
					"coordinatorID", resp.CoordinatorID,
					"address", addr,
					"error", err)
				lastErr = err
				continue
			}
			b.conns[resp.CoordinatorID] = conn
			return conn, nil
		}

		// if none of the connections worked out, try with fresh data
		if err := b.refreshMetadata(ctx, proto.MetadataV0); isCanceled(err) {
			return nil, err
		} else if err != nil {
			logger.Debug("cannot refresh metadata",
				"error", err)
			lastErr = err
			continue
		}

		for nodeID, addr := range b.metadata.nodes {
			if _, ok := b.conns[nodeID]; ok {
				// connection to node is cached so it was already checked
				continue
			}
			conn, err := b.newConnection(ctx, addr)
			if isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Debug("cannot connect to node",
					"nodeID", nodeID,
					"address", addr,
					"error", err)
				lastErr = err
				continue
			}
			b.conns[nodeID] = conn

			resp, err := conn.ConsumerMetadata(ctx, b.conf.ConsumerMetadataTimeout, &proto.ConsumerMetadataReq{
				ClientID:      b.conf.ClientID,
				ConsumerGroup: consumerGroup,
			})
			if isCloseDeadConnectionNeeded(err) {
				// Note: We're already locked
				logger.Info("Closing dead connection",
					"nodeID", nodeID)
				b.closeDeadConnection(ctx, conn, false)
			}
			if isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Debug("cannot fetch metadata",
					"consumGrp", consumerGroup,
					"error", err)
				lastErr = err
				continue
			}
			if resp.Err != nil {
				logger.Debug("metadata response error",
					"consumGrp", consumerGroup,
					"error", resp.Err)
				lastErr = err
				continue
			}

			addr := net.JoinHostPort(resp.CoordinatorHost, strconv.Itoa(int(resp.CoordinatorPort)))
			conn, err = b.newConnection(ctx, addr)
			if isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Debug("cannot connect to node",
					"coordinatorID", resp.CoordinatorID,
					"address", addr,
					"error", err)
				lastErr = err
				continue
			}
			b.conns[resp.CoordinatorID] = conn
			return conn, nil
		}
		lastErr = proto.ErrNoCoordinator
	}
	return nil, lastErr
}

// muControllerConnection returns connection to controller of the cluster.
//
// Failed connection retry is controlled by broker configuration.
func (b *Broker) muControllerConnection(ctx context.Context) (*connection, error) {
	logger := getLogFromContext(ctx, b.conf.Logger)
	b.mu.Lock()
	defer b.mu.Unlock()
	lastErr := fmt.Errorf("cannot get controller connection")

	for retry := 0; retry < b.conf.LeaderRetryLimit; retry++ {
		if err := ctx.Err(); err != nil {
			b.conf.Logger.Debug("context canceled")
			return nil, err
		}

		if retry != 0 {
			func() {
				b.mu.Unlock()
				defer b.mu.Lock()
				logger.Debug("cannot get controller connection",
					"retry", retry,
					"sleep", b.conf.LeaderRetryWait.String())
				select {
				case <-time.After(b.conf.LeaderRetryWait):
					// continue
				case <-ctx.Done():
					// context canceled
				}
			}()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}

		controllerID := b.metadata.controllerID
		if controllerID == nil {
			if err := b.refreshMetadata(ctx, proto.MetadataV1); isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Info("cannot get controller connection: cannot refresh metadata",
					"error", err)
				lastErr = err
				continue
			}
			controllerID = b.metadata.controllerID
			if controllerID == nil {
				logger.Info("cannot get controller connection: unknown controller ID")
				continue
			}
		}
		nodeID := *controllerID

		conn, ok := b.conns[nodeID]
		if !ok {
			addr, ok := b.metadata.nodes[nodeID]
			if !ok {
				logger.Info("cannot get controller connection: no information about node",
					"nodeID", nodeID)
				continue
			}
			var err error
			conn, err = b.newConnection(ctx, addr)
			if isCanceled(err) {
				return nil, err
			} else if err != nil {
				logger.Info("cannot get controller connection: cannot connect to node",
					"address", addr,
					"error", err)
				lastErr = err
				continue
			}
			b.conns[nodeID] = conn
		}
		return conn, nil
	}
	return nil, lastErr
}

// muCloseDeadConnection is closing and removing any reference to given
// connection. Because we remove dead connection, additional request to refresh
// metadata is made
//
// muCloseDeadConnection call is protected with broker's lock.
func (b *Broker) muCloseDeadConnection(ctx context.Context, conn *connection) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closeDeadConnection(ctx, conn, true)
}

// closeDeadConnection is closing and removing any reference to given
// connection. Because we remove dead connection, additional optional request to refresh
// metadata is made
//
// Because it's modifying connection state it's not thread safe and using it require locking.
func (b *Broker) closeDeadConnection(ctx context.Context, conn *connection, refreshMetadata bool) {
	logger := getLogFromContext(ctx, b.conf.Logger)

	for nid, c := range b.conns {
		if c == conn {
			logger.Debug("closing dead connection",
				"nodeID", nid)
			delete(b.conns, nid)
			if b.metadata.controllerID != nil && *b.metadata.controllerID == nid {
				b.metadata.controllerID = nil
			}
			go c.Close()
			if refreshMetadata {
				if err := b.refreshMetadata(context.Background(), proto.MetadataV0); err != nil {
					logger.Debug("cannot refresh metadata",
						"error", err)
				}
			}
			return
		}
	}
}

// offset will return offset value for given partition. Use timems to specify
// which offset value should be returned.
func (b *Broker) offset(ctx context.Context, topic string, partition int32, timems int64) (offset int64, err error) {
	logger := getLogFromContext(ctx, b.conf.Logger)
	for retry := 0; retry < b.conf.RetryErrLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return 0, err
		}

		if retry != 0 {
			select {
			case <-time.After(b.conf.RetryErrWait):
				// continue
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			err = b.muRefreshMetadata(ctx, proto.MetadataV0)
			if err != nil {
				continue
			}
		}
		var conn *connection
		conn, err = b.muLeaderConnection(ctx, topic, partition)
		if err != nil {
			return 0, err
		}
		var resp *proto.OffsetResp
		resp, err = conn.Offset(ctx, b.conf.OffsetTimeout, &proto.OffsetReq{
			ClientID:  b.conf.ClientID,
			ReplicaID: -1, // any client
			Topics: []proto.OffsetReqTopic{
				{
					Name: topic,
					Partitions: []proto.OffsetReqPartition{
						{
							ID:         partition,
							TimeMs:     timems,
							MaxOffsets: 2,
						},
					},
				},
			},
		})
		if err != nil {
			if isCloseDeadConnectionNeeded(err) {
				// Connection is broken, so should be closed, but the error is
				// still valid and should be returned so that retry mechanism have
				// chance to react.
				logger.Debug("connection died while fetching offsets",
					"topic", topic,
					"partition", partition,
					"error", err)
				b.muCloseDeadConnection(ctx, conn)
			}
			continue
		}
		for _, t := range resp.Topics {
			if t.Name != topic {
				logger.Debug("unexpected topic information",
					"expected", topic,
					"got", t.Name)
				continue
			}
			for _, part := range t.Partitions {
				if part.ID != partition {
					logger.Debug("unexpected partition information",
						"topic", t.Name,
						"expected", partition,
						"got", part.ID)
					continue
				}
				if err = part.Err; err == nil {
					if len(part.Offsets) == 0 {
						return 0, nil
					} else {
						return part.Offsets[0], nil
					}
				}
			}
		}
	}
	return 0, errors.New("incomplete fetch response")
}

// OffsetEarliest returns the oldest offset available on the given partition.
func (b *Broker) OffsetEarliest(ctx context.Context, topic string, partition int32) (offset int64, err error) {
	return b.offset(ctx, topic, partition, -2)
}

// OffsetLatest return the offset of the next message produced in given partition
func (b *Broker) OffsetLatest(ctx context.Context, topic string, partition int32) (offset int64, err error) {
	return b.offset(ctx, topic, partition, -1)
}

// ProducerConf represents the configuration of a producer.
type ProducerConf struct {
	// Compression method to use, defaulting to proto.CompressionNone.
	Compression proto.Compression

	// Message ACK configuration. Use proto.RequiredAcksAll to require all
	// servers to write, proto.RequiredAcksLocal to wait only for leader node
	// answer or proto.RequiredAcksNone to not wait for any response.
	// Setting this to any other, greater than zero value will make producer to
	// wait for given number of servers to confirm write before returning.
	RequiredAcks int16

	// Timeout of single produce request. By default, 5 seconds.
	RequestTimeout time.Duration

	// RetryLimit specify how many times message producing should be retried in
	// case of failure, before returning the error to the caller. By default
	// set to 10.
	RetryLimit int

	// RetryWait specify wait duration before produce retry after failure. By
	// default set to 200ms.
	RetryWait time.Duration

	// ProduceTimeout is the maximum time a Produce request is allowed to take.
	ProduceTimeout time.Duration

	// Logger used by producer. By default, reuse logger assigned to broker.
	Logger Logger
}

// NewProducerConf returns a default producer configuration.
func NewProducerConf() ProducerConf {
	return ProducerConf{
		Compression:    proto.CompressionNone,
		RequiredAcks:   proto.RequiredAcksAll,
		RequestTimeout: 5 * time.Second,
		RetryLimit:     10,
		RetryWait:      200 * time.Millisecond,
		ProduceTimeout: time.Second * 20,
		Logger:         nil,
	}
}

// producer is the link to the client with extra configuration.
type producer struct {
	conf   ProducerConf
	broker *Broker
}

// Producer returns new producer instance, bound to the broker.
func (b *Broker) Producer(conf ProducerConf) Producer {
	if conf.Logger == nil {
		conf.Logger = b.conf.Logger
	}
	return &producer{
		conf:   conf,
		broker: b,
	}
}

// Produce writes messages to the given destination. Writes within the call are
// atomic, meaning either all or none of them are written to kafka.  Produce
// has a configurable amount of retries which may be attempted when common
// errors are encountered.  This behaviour can be configured with the
// RetryLimit and RetryWait attributes.
//
// Upon a successful call, the message's Offset field is updated.
func (p *producer) Produce(ctx context.Context, topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {
	logger := getLogFromContext(ctx, p.conf.Logger)

	for retry := 0; retry < p.conf.RetryLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return 0, err
		}

		if retry != 0 {
			select {
			case <-time.After(p.conf.RetryWait):
				// continue
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		offset, err = p.produce(ctx, topic, partition, messages...)
		if isCanceled(err) {
			return 0, err
		}

		switch err {
		case nil:
			for i, msg := range messages {
				msg.Offset = int64(i) + offset
			}
			return offset, err
		case io.EOF, syscall.EPIPE:
			// p.produce call is closing connection when this error shows up,
			// but it's also returning it so that retry loop can count this
			// case
			// we cannot handle this error here, because there is no direct
			// access to connection
		default:
			logger.Debug("Produce: calling muRefreshMetadata",
				"retry", retry,
				"error", err)
			if err := p.broker.muRefreshMetadata(ctx, proto.MetadataV0); isCanceled(err) {
				return 0, err
			} else if err != nil {
				logger.Debug("cannot refresh metadata",
					"error", err)
			}
		}
		logger.Debug("cannot produce messages",
			"retry", retry,
			"error", err)
	}

	return 0, err

}

// produce send produce request to leader for given destination.
func (p *producer) produce(ctx context.Context, topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {
	logger := getLogFromContext(ctx, p.conf.Logger)
	conn, err := p.broker.muLeaderConnection(ctx, topic, partition)
	if err != nil {
		return 0, err
	}

	req := proto.ProduceReq{
		ClientID:     p.broker.conf.ClientID,
		Compression:  p.conf.Compression,
		RequiredAcks: p.conf.RequiredAcks,
		Timeout:      p.conf.RequestTimeout,
		Topics: []proto.ProduceReqTopic{
			{
				Name: topic,
				Partitions: []proto.ProduceReqPartition{
					{
						ID:       partition,
						Messages: messages,
					},
				},
			},
		},
	}

	resp, err := conn.Produce(ctx, p.conf.ProduceTimeout, &req)
	if err != nil {
		if isCloseDeadConnectionNeeded(err) {
			// Connection is broken, so should be closed, but the error is
			// still valid and should be returned so that retry mechanism have
			// chance to react.
			logger.Debug("connection died while sending message",
				"topic", topic,
				"partition", partition,
				"error", err)
			go p.broker.muCloseDeadConnection(ctx, conn)
		}
		return 0, err
	}

	if req.RequiredAcks == proto.RequiredAcksNone {
		return 0, err
	}

	// we expect single partition response
	found := false
	for _, t := range resp.Topics {
		if t.Name != topic {
			logger.Debug("unexpected topic information received",
				"expected", topic,
				"got", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				logger.Debug("unexpected partition information received",
					"topic", t.Name,
					"expected", partition,
					"got", part.ID)
				continue
			}
			found = true
			offset = part.Offset
			err = part.Err
		}
	}

	if !found {
		return 0, errors.New("incomplete produce response")
	}
	return offset, err
}

// ConsumerConf represents the configuration of a consumer.
type ConsumerConf struct {
	// Topic name that should be consumed
	Topic string

	// Partition ID that should be consumed.
	Partition int32

	// RequestTimeout controls fetch request timeout. This operation is
	// blocking the whole connection, so it should always be set to a small
	// value. By default it's set to 50ms.
	// To control fetch function timeout use RetryLimit and RetryWait.
	RequestTimeout time.Duration

	// RetryLimit limits fetching messages a given amount of times before
	// returning ErrNoData error.
	//
	// Default is -1, which turns this limit off.
	RetryLimit int

	// RetryWait controls the duration of wait between fetch request calls,
	// when no data was returned.
	//
	// Default is 50ms.
	RetryWait time.Duration

	// RetryErrLimit limits the number of retry attempts when an error is
	// encountered.
	//
	// Default is 10.
	RetryErrLimit int

	// RetryErrWait controls the wait duration between retries after failed
	// fetch request.
	//
	// Default is 500ms.
	RetryErrWait time.Duration

	// MinFetchSize is the minimum size of messages to fetch in bytes.
	//
	// Default is 1 to fetch any message available.
	MinFetchSize int32

	// MaxFetchSize is the maximum size of data which can be sent by kafka node
	// to consumer.
	//
	// Default is 2000000 bytes.
	MaxFetchSize int32

	// Consumer cursor starting point. Set to StartOffsetNewest to receive only
	// newly created messages or StartOffsetOldest to read everything. Assign
	// any offset value to manually set cursor -- consuming starts with the
	// message whose offset is equal to given value (including first message).
	//
	// Default is StartOffsetOldest.
	StartOffset int64

	// FetchTimeout is the maximum time a Fetch request is allowed to take.
	FetchTimeout time.Duration

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Logger Logger
}

// NewConsumerConf returns the default consumer configuration.
func NewConsumerConf(topic string, partition int32) ConsumerConf {
	return ConsumerConf{
		Topic:          topic,
		Partition:      partition,
		RequestTimeout: time.Millisecond * 50,
		RetryLimit:     -1,
		RetryWait:      time.Millisecond * 50,
		RetryErrLimit:  10,
		RetryErrWait:   time.Millisecond * 500,
		MinFetchSize:   1,
		MaxFetchSize:   2000000,
		StartOffset:    StartOffsetOldest,
		FetchTimeout:   time.Second * 10,
		Logger:         nil,
	}
}

// Consumer represents a single partition reading buffer. Consumer is also
// providing limited failure handling and message filtering.
type consumer struct {
	broker *Broker
	conf   ConsumerConf

	mu_    sync.Mutex
	offset int64 // offset of next NOT consumed message
	conn_  *connection
	msgbuf []*proto.Message
}

// Consumer creates a new consumer instance, bound to the broker.
func (b *Broker) Consumer(ctx context.Context, conf ConsumerConf) (Consumer, error) {
	return b.consumer(ctx, conf)
}

// BatchConsumer creates a new BatchConsumer instance, bound to the broker.
func (b *Broker) BatchConsumer(ctx context.Context, conf ConsumerConf) (BatchConsumer, error) {
	return b.consumer(ctx, conf)
}

func (b *Broker) consumer(ctx context.Context, conf ConsumerConf) (*consumer, error) {
	conn, err := b.muLeaderConnection(ctx, conf.Topic, conf.Partition)
	if err != nil {
		return nil, err
	}
	if conf.Logger == nil {
		conf.Logger = b.conf.Logger
	}
	offset := conf.StartOffset
	if offset < 0 {
		switch offset {
		case StartOffsetNewest:
			off, err := b.OffsetLatest(ctx, conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		case StartOffsetOldest:
			off, err := b.OffsetEarliest(ctx, conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		default:
			return nil, fmt.Errorf("invalid start offset: %d", conf.StartOffset)
		}
	}
	c := &consumer{
		broker: b,
		conn_:  conn,
		conf:   conf,
		msgbuf: make([]*proto.Message, 0),
		offset: offset,
	}
	return c, nil
}

// leaderConnection returns the leader connection for the topic, creating one if needed.
// Mutex must be owned when calling this function.
func (c *consumer) leaderConnection(ctx context.Context) (*connection, error) {
	if conn := c.conn_; conn != nil {
		return conn, nil
	}
	conn, err := c.broker.muLeaderConnection(ctx, c.conf.Topic, c.conf.Partition)
	if err != nil {
		return nil, err
	}
	c.conn_ = conn
	return conn, nil
}

// closeDeadConnection closes the connection, if it is still the current connection.
// Mutex must be owned when calling this function.
func (c *consumer) closeDeadConnection(ctx context.Context, conn *connection) {
	c.conn_ = nil
	go c.broker.muCloseDeadConnection(ctx, conn)
}

// consume is returning a batch of messages from consumed partition.
// Consumer can retry fetching messages even if responses return no new
// data. Retry behaviour can be configured through RetryLimit and RetryWait
// consumer parameters.
//
// consume can retry sending request on common errors. This behaviour can
// be configured with RetryErrLimit and RetryErrWait consumer configuration
// attributes.
func (c *consumer) consume(ctx context.Context) ([]*proto.Message, error) {
	logger := getLogFromContext(ctx, c.conf.Logger)
	var msgbuf []*proto.Message
	var retry int
	for len(msgbuf) == 0 {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return nil, err
		}

		var err error
		msgbuf, err = c.fetch(ctx)
		if err != nil {
			return nil, err
		}
		if len(msgbuf) == 0 {
			if c.conf.RetryWait > 0 {
				select {
				case <-time.After(c.conf.RetryWait):
					// continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			retry++
			if c.conf.RetryLimit != -1 && retry > c.conf.RetryLimit {
				return nil, ErrNoData
			}
		}
	}

	return msgbuf, nil
}

func (c *consumer) Consume(ctx context.Context) (*proto.Message, error) {
	c.mu_.Lock()
	defer c.mu_.Unlock()

	if len(c.msgbuf) == 0 {
		var err error
		c.msgbuf, err = c.consume(ctx)
		if err != nil {
			return nil, err
		}
	}

	msg := c.msgbuf[0]
	c.msgbuf = c.msgbuf[1:]
	c.offset = msg.Offset + 1
	return msg, nil
}

func (c *consumer) ConsumeBatch(ctx context.Context) ([]*proto.Message, error) {
	c.mu_.Lock()
	defer c.mu_.Unlock()

	batch, err := c.consume(ctx)
	if err != nil {
		return nil, err
	}
	c.offset = batch[len(batch)-1].Offset + 1

	return batch, nil
}

// fetch and return next batch of messages. In case of certain set of errors,
// retry sending fetch request. Retry behaviour can be configured with
// RetryErrLimit and RetryErrWait consumer configuration attributes.
func (c *consumer) fetch(ctx context.Context) ([]*proto.Message, error) {
	logger := getLogFromContext(ctx, c.conf.Logger)
	req := proto.FetchReq{
		ClientID:    c.broker.conf.ClientID,
		MaxWaitTime: c.conf.RequestTimeout,
		MinBytes:    c.conf.MinFetchSize,
		Topics: []proto.FetchReqTopic{
			{
				Name: c.conf.Topic,
				Partitions: []proto.FetchReqPartition{
					{
						ID:          c.conf.Partition,
						FetchOffset: c.offset,
						MaxBytes:    c.conf.MaxFetchSize,
					},
				},
			},
		},
	}

	var resErr error
consumeRetryLoop:
	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return nil, err
		}

		if retry != 0 {
			select {
			case <-time.After(c.conf.RetryErrWait):
				// continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if retry > 0 {
			logger.Debug("fetching leader connection for fetching message",
				"retry", retry,
				"topic", c.conf.Topic,
				"partition", c.conf.Partition)
		}
		conn, err := c.leaderConnection(ctx)
		if retry > 0 {
			logger.Debug("fetched leader connection for fetching message",
				"retry", retry,
				"topic", c.conf.Topic,
				"partition", c.conf.Partition,
				"error", err)
		}
		if isCanceled(err) {
			return nil, err
		} else if err != nil {
			resErr = err
			continue
		}

		resp, err := conn.Fetch(ctx, c.conf.FetchTimeout, &req)
		resErr = err

		if isCloseDeadConnectionNeeded(err) {
			logger.Debug("connection died while fetching message",
				"retry", retry,
				"topic", c.conf.Topic,
				"partition", c.conf.Partition,
				"error", err)
			c.closeDeadConnection(ctx, conn)
			continue
		}

		if err != nil {
			logger.Debug("cannot fetch messages: unknown error",
				"retry", retry,
				"error", err)
			c.closeDeadConnection(ctx, conn)
			continue
		}

		for _, topic := range resp.Topics {
			if topic.Name != c.conf.Topic {
				logger.Warn("unexpected topic information received",
					"got", topic.Name,
					"expected", c.conf.Topic)
				continue
			}
			for _, part := range topic.Partitions {
				if part.ID != c.conf.Partition {
					logger.Warn("unexpected partition information received",
						"topic", topic.Name,
						"expected", c.conf.Partition,
						"got", part.ID)
					continue
				}
				switch part.Err {
				case proto.ErrLeaderNotAvailable,
					proto.ErrNotLeaderForPartition,
					proto.ErrBrokerNotAvailable,
					proto.ErrUnknownTopicOrPartition:

					logger.Debug("cannot fetch messages",
						"retry", retry,
						"error", part.Err)
					if err := c.broker.muRefreshMetadata(ctx, proto.MetadataV0); err != nil {
						logger.Debug("cannot refresh metadata",
							"error", err)
					}
					// The connection is fine, so don't close it,
					// but we may very well need to talk to a different broker now.
					// Set the conn to nil so that next time around the loop
					// we'll check the metadata again to see who we're supposed to talk to.
					c.conn_ = nil
					continue consumeRetryLoop
				}
				if len(part.Messages) > 0 && retry > 0 {
					logger.Debug("returning fetched messages",
						"retry", retry,
						"topic", c.conf.Topic,
						"partition", c.conf.Partition)
				}
				return part.Messages, part.Err
			}
		}
		return nil, errors.New("incomplete fetch response")
	}

	return nil, resErr
}

// OffsetCoordinatorConf represents the configuration of an offset coordinator.
type OffsetCoordinatorConf struct {
	ConsumerGroup string

	// RetryErrLimit limits messages fetch retry upon failure. By default 10.
	RetryErrLimit int

	// RetryErrWait controls wait duration between retries after failed fetch
	// request. By default 500ms.
	RetryErrWait time.Duration

	// CommitTimeout control the maximum duration of a Commit request.
	// By default 10s
	CommitTimeout time.Duration

	// OffsetFetchTimeout control the maximum duration of a OffsetFetch request.
	// By default 10s
	OffsetFetchTimeout time.Duration

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Logger Logger
}

// NewOffsetCoordinatorConf returns default OffsetCoordinator configuration.
func NewOffsetCoordinatorConf(consumerGroup string) OffsetCoordinatorConf {
	return OffsetCoordinatorConf{
		ConsumerGroup:      consumerGroup,
		RetryErrLimit:      10,
		RetryErrWait:       time.Millisecond * 500,
		CommitTimeout:      time.Second * 10,
		OffsetFetchTimeout: time.Second * 10,
		Logger:             nil,
	}
}

type offsetCoordinator struct {
	conf   OffsetCoordinatorConf
	broker *Broker

	mu   sync.Mutex
	conn *connection
}

// OffsetCoordinator returns offset management coordinator for single consumer
// group, bound to broker.
func (b *Broker) OffsetCoordinator(ctx context.Context, conf OffsetCoordinatorConf) (OffsetCoordinator, error) {
	conn, err := b.muCoordinatorConnection(ctx, conf.ConsumerGroup)
	if err != nil {
		return nil, err
	}
	if conf.Logger == nil {
		conf.Logger = b.conf.Logger
	}
	c := &offsetCoordinator{
		broker: b,
		conf:   conf,
		conn:   conn,
	}
	return c, nil
}

// muCoordinatorConnection returns the currently open coordinator connection,
// or a new coordinator connection is no such connection is available.
func (c *offsetCoordinator) muCoordinatorConnection(ctx context.Context) (*connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := c.broker.muCoordinatorConnection(ctx, c.conf.ConsumerGroup)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

// muCloseDeadConnection claims the mutex and if the given connection is the current connection, it
// is closed and removed from the broker.
func (c *offsetCoordinator) muCloseDeadConnection(ctx context.Context, conn *connection) {
	if conn != nil {
		c.mu.Lock()
		defer c.mu.Unlock()

		if c.conn == conn {
			c.conn = nil
			go c.broker.muCloseDeadConnection(ctx, conn)
		}
	}
}

// Commit is saving offset information for given topic and partition.
//
// Commit can retry saving offset information on common errors. This behaviour
// can be configured with with RetryErrLimit and RetryErrWait coordinator
// configuration attributes.
func (c *offsetCoordinator) Commit(ctx context.Context, topic string, partition int32, offset int64) error {
	return c.commit(ctx, topic, partition, offset, "")
}

// Commit works exactly like Commit method, but store extra metadata string
// together with offset information.
func (c *offsetCoordinator) CommitFull(ctx context.Context, topic string, partition int32, offset int64, metadata string) error {
	return c.commit(ctx, topic, partition, offset, metadata)
}

// commit is saving offset and metadata information. Provides limited error
// handling configurable through OffsetCoordinatorConf.
func (c *offsetCoordinator) commit(ctx context.Context, topic string, partition int32, offset int64, metadata string) (resErr error) {
	logger := getLogFromContext(ctx, c.conf.Logger)

	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return err
		}

		if retry != 0 {
			select {
			case <-time.After(c.conf.RetryErrWait):
				// continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Fetch connection
		conn, err := c.muCoordinatorConnection(ctx)
		if isCanceled(err) {
			return err
		} else if err != nil {
			resErr = err
			logger.Debug("cannot connect to coordinator",
				"consumGrp", c.conf.ConsumerGroup,
				"error", err)
			continue
		}

		resp, err := conn.OffsetCommit(ctx, c.conf.CommitTimeout, &proto.OffsetCommitReq{
			ClientID:      c.broker.conf.ClientID,
			ConsumerGroup: c.conf.ConsumerGroup,
			Topics: []proto.OffsetCommitReqTopic{
				{
					Name: topic,
					Partitions: []proto.OffsetCommitReqPartition{
						{ID: partition, Offset: offset, TimeStamp: time.Now(), Metadata: metadata},
					},
				},
			},
		})
		resErr = err

		if isCloseDeadConnectionNeeded(err) {
			logger.Debug("connection died while commiting",
				"topic", topic,
				"partition", partition,
				"consumGrp", c.conf.ConsumerGroup)
			c.muCloseDeadConnection(ctx, conn)
		} else if err == nil {
			for _, t := range resp.Topics {
				if t.Name != topic {
					logger.Debug("unexpected topic information received",
						"got", t.Name,
						"expected", topic)
					continue

				}
				for _, part := range t.Partitions {
					if part.ID != partition {
						logger.Debug("unexpected partition information received",
							"topic", topic,
							"got", part.ID,
							"expected", partition)
						continue
					}
					return part.Err
				}
			}
			return errors.New("response does not contain commit information")
		}
	}
	return resErr
}

// Offset is returning last offset and metadata information committed for given
// topic and partition.
// Offset can retry sending request on common errors. This behaviour can be
// configured with with RetryErrLimit and RetryErrWait coordinator
// configuration attributes.
func (c *offsetCoordinator) Offset(ctx context.Context, topic string, partition int32) (offset int64, metadata string, resErr error) {
	logger := getLogFromContext(ctx, c.conf.Logger)

	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return 0, "", err
		}

		if retry != 0 {
			select {
			case <-time.After(c.conf.RetryErrWait):
				// continue
			case <-ctx.Done():
				return 0, "", ctx.Err()
			}
		}

		// connection can be set to nil if previously reference connection died
		conn, err := c.muCoordinatorConnection(ctx)
		if isCanceled(err) {
			return 0, "", err
		} else if err != nil {
			logger.Debug("cannot connect to coordinator",
				"consumGrp", c.conf.ConsumerGroup,
				"error", err)
			resErr = err
			continue
		}

		resp, err := conn.OffsetFetch(ctx, c.conf.OffsetFetchTimeout, &proto.OffsetFetchReq{
			ConsumerGroup: c.conf.ConsumerGroup,
			Topics: []proto.OffsetFetchReqTopic{
				{
					Name:       topic,
					Partitions: []int32{partition},
				},
			},
		})
		resErr = err

		if isCloseDeadConnectionNeeded(err) {
			logger.Debug("connection died while fetching offset",
				"topic", topic,
				"partition", partition,
				"consumGrp", c.conf.ConsumerGroup)
			c.muCloseDeadConnection(ctx, conn)
		} else if err == nil {
			for _, t := range resp.Topics {
				if t.Name != topic {
					logger.Debug("unexpected topic information received",
						"got", t.Name,
						"expected", topic)
					continue
				}
				for _, part := range t.Partitions {
					if part.ID != partition {
						logger.Debug("unexpected partition information received",
							"topic", topic,
							"expected", partition,
							"get", part.ID)
						continue
					}
					if part.Err != nil {
						return 0, "", part.Err
					}
					return part.Offset, part.Metadata, nil
				}
			}
			return 0, "", errors.New("response does not contain offset information")
		}
	}

	return 0, "", resErr
}

// AdminConf represents the configuration of an admin-client.
type AdminConf struct {
	// RetryErrLimit limits messages fetch retry upon failure. By default 10.
	RetryErrLimit int

	// RetryErrWait controls wait duration between retries after failed fetch
	// request. By default 500ms.
	RetryErrWait time.Duration

	// DescribeConfigsTimeout is the maximum time a DescribeConfigs request can take.
	DescribeConfigsTimeout time.Duration

	// DeleteTopicsTimeout is the maximum time a DeleteTopics request can take.
	DeleteTopicsTimeout time.Duration

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Logger Logger
}

// NewAdminConf returns default AdminConf configuration.
func NewAdminConf() AdminConf {
	return AdminConf{
		RetryErrLimit:          10,
		RetryErrWait:           time.Millisecond * 500,
		DescribeConfigsTimeout: time.Second * 10,
		DeleteTopicsTimeout:    time.Second * 30,
		Logger:                 nil,
	}
}

type admin struct {
	conf   AdminConf
	broker *Broker

	mu_   sync.Mutex
	conn_ *connection
}

// Admin returns and admin-client for the cluster.
func (b *Broker) Admin(ctx context.Context, conf AdminConf) (Admin, error) {
	conn, err := b.muControllerConnection(ctx)
	if err != nil {
		return nil, err
	}
	if conf.Logger == nil {
		conf.Logger = b.conf.Logger
	}
	c := &admin{
		broker: b,
		conf:   conf,
		conn_:  conn,
	}
	return c, nil
}

// muControllerConnection returns the currently open controller connection,
// or a new coordinator connection is no such connection is available.
func (c *admin) muControllerConnection(ctx context.Context) (*connection, error) {
	c.mu_.Lock()
	defer c.mu_.Unlock()

	if c.conn_ != nil {
		return c.conn_, nil
	}
	conn, err := c.broker.muControllerConnection(ctx)
	if err != nil {
		return nil, err
	}
	c.conn_ = conn
	return conn, nil
}

// muCloseDeadConnection claims the mutex and if the given connection is the current connection, it
// is closed and removed from the broker.
func (c *admin) muCloseDeadConnection(ctx context.Context, conn *connection) {
	if conn != nil {
		c.mu_.Lock()
		defer c.mu_.Unlock()

		if c.conn_ == conn {
			c.conn_ = nil
			go c.broker.muCloseDeadConnection(ctx, conn)
		}
	}
}

// DeleteTopics removes a given set of topics from the cluster.
func (c *admin) DeleteTopics(ctx context.Context, topics []string, timeout int32) ([]proto.TopicErrorCode, error) {
	logger := getLogFromContext(ctx, c.conf.Logger)

	lastErr := fmt.Errorf("DeleteTopics failed")
	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return nil, err
		}

		if retry != 0 {
			select {
			case <-time.After(c.conf.RetryErrWait):
				// continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// connection can be set to nil if previously reference connection died
		conn, err := c.muControllerConnection(ctx)
		if isCanceled(err) {
			return nil, err
		} else if err != nil {
			logger.Debug("cannot connect to coordinator",
				"error", err)
			lastErr = err
			continue
		}

		resp, err := conn.DeleteTopics(ctx, c.conf.DeleteTopicsTimeout, &proto.DeleteTopicsReq{
			Topics:  topics,
			Timeout: timeout,
		})

		if isCloseDeadConnectionNeeded(err) {
			logger.Debug("connection died while deleting topics")
			c.muCloseDeadConnection(ctx, conn)
			lastErr = err
		} else if err == nil {
			return resp.TopicErrorCodes, nil
		} else {
			lastErr = err
		}
	}

	return nil, lastErr
}

func (c *admin) DescribeConfigs(ctx context.Context, configs ...proto.ConfigResource) ([]proto.ConfigResourceEntry, error) {
	logger := getLogFromContext(ctx, c.conf.Logger)

	lastErr := fmt.Errorf("DescribeConfigs failed")
	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		if err := ctx.Err(); err != nil {
			logger.Debug("context canceled")
			return nil, err
		}

		if retry != 0 {
			select {
			case <-time.After(c.conf.RetryErrWait):
				// continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// connection can be set to nil if previously reference connection died
		conn, err := c.muControllerConnection(ctx)
		if isCanceled(err) {
			return nil, err
		} else if err != nil {
			logger.Debug("cannot connect to coordinator",
				"error", err)
			lastErr = err
			continue
		}

		resp, err := conn.DescribeConfigs(ctx, c.conf.DescribeConfigsTimeout, &proto.DescribeConfigsReq{
			Resources: configs,
		})

		if isCloseDeadConnectionNeeded(err) {
			logger.Debug("connection died while describing configs")
			c.muCloseDeadConnection(ctx, conn)
			lastErr = err
		} else if err == nil {
			return resp.Resources, nil
		} else {
			lastErr = err
		}
	}

	return nil, lastErr
}

// isCloseDeadConnectionNeeded inspects the given error and returns true if
// muCloseDeadConnection must be called.
func isCloseDeadConnectionNeeded(err error) bool {
	_, ok := err.(*net.OpError)
	return ok || err == io.EOF || err == syscall.EPIPE || err == proto.ErrRequestTimeout || isCanceled(err)
}
