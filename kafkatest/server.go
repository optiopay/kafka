package kafkatest

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"sync"

	"github.com/optiopay/kafka/proto"
)

const (
	AnyRequest              = -1
	ProduceRequest          = 0
	FetchRequest            = 1
	OffsetRequest           = 2
	MetadataRequest         = 3
	OffsetCommitRequest     = 8
	OffsetFetchRequest      = 9
	ConsumerMetadataRequest = 10
)

type Serializable interface {
	Bytes() ([]byte, error)
}

type RequestHandler func(request Serializable) (response Serializable)

type Logs struct {
	mu   sync.Mutex
	logs map[string]map[int32][]*proto.Message
}

func (lg *Logs) AddPartition(topic string, partitions ...int32) {
	lg.mu.Lock()
	for _, part := range partitions {
		lg.addPartition(topic, part)
	}
	lg.mu.Unlock()
}

func (lg *Logs) addPartition(topic string, partition int32) {
	parts, ok := lg.logs[topic]
	if !ok {
		parts = make(map[int32][]*proto.Message)
		lg.logs[topic] = parts
	}
	if _, ok := parts[partition]; !ok {
		messages := make([]*proto.Message, 0)
		parts[partition] = messages
	}
}

func (lg *Logs) HasPartition(topic string, partition int32) bool {
	lg.mu.Lock()
	ok := lg.hasPartition(topic, partition)
	lg.mu.Unlock()
	return ok
}

func (lg *Logs) PartitionOffset(topic string, partition int32) int64 {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if !lg.hasPartition(topic, partition) {
		return -1
	}
	msgs := lg.logs[topic][partition]
	if len(msgs) == 0 {
		return 0
	}
	return msgs[len(msgs)-1].Offset
}

func (lg *Logs) HasTopic(topic string) bool {
	lg.mu.Lock()
	_, ok := lg.logs[topic]
	lg.mu.Unlock()
	return ok
}

func (lg *Logs) hasPartition(topic string, partition int32) bool {
	parts, ok := lg.logs[topic]
	if !ok {
		return false
	}
	_, ok = parts[partition]
	return ok
}

// AddMessage appends message to log if log array exists. Returns error if it
// does not.
func (lg *Logs) AddMessages(topic string, partition int32, messages []*proto.Message) (offset int64, err error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if !lg.hasPartition(topic, partition) {
		return -1, proto.ErrUnknownTopicOrPartition
	}
	mlog := lg.logs[topic][partition]
	offset = int64(len(mlog) + 1)
	for _, m := range messages {
		m.Crc = computeCrc(m)
		m.Offset = int64(len(mlog)) + 1
		mlog = append(mlog, m)
	}
	lg.logs[topic][partition] = mlog
	return offset, nil
}

func computeCrc(m *proto.Message) uint32 {
	var buf bytes.Buffer
	enc := proto.NewEncoder(&buf)
	enc.Encode(int8(0)) // magic byte is always 0
	enc.Encode(int8(0)) // no compression support
	enc.Encode(m.Key)
	enc.Encode(m.Value)
	return crc32.ChecksumIEEE(buf.Bytes())
}

// Messages returns messages for given topic and partition with offset starting
// with requested.
// To imitate kafka, it might happend that last message in message set will be
// empty and that message set will start with messages of index lower than
// requested
func (lg *Logs) Messages(topic string, partition int32, offset int64, limit int) ([]*proto.Message, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if !lg.hasPartition(topic, partition) {
		return nil, proto.ErrUnknownTopicOrPartition
	}
	messages := lg.logs[topic][partition]
	if offset > int64(len(messages)) {
		return nil, proto.ErrOffsetOutOfRange
	}
	safeLimit := limit
	if safeLimit > len(messages) {
		safeLimit = len(messages)
	}

	// imitate kafka's compression that may return messages with lower id than requested
	if offset > 1 {
		offset -= 1
	}

	result := messages[offset:safeLimit]

	// imitiate kafka's optimization that is adding empty message to the end of messages set
	result = append(result, &proto.Message{})

	return result, nil
}

type Server struct {
	Processed int
	Logs      *Logs

	mu       sync.RWMutex
	ln       net.Listener
	handlers map[int16]RequestHandler
}

func NewServer() *Server {
	srv := &Server{
		Logs: &Logs{
			logs: make(map[string]map[int32][]*proto.Message),
		},
		handlers: make(map[int16]RequestHandler),
	}
	srv.handlers[AnyRequest] = srv.defaultRequestHandler
	return srv
}

// Handle registers handler for given message kind. Handler registered with
// AnyRequest kind will be used only if there is no precise handler for the
// kind.
func (srv *Server) Handle(reqKind int16, handler RequestHandler) {
	srv.mu.Lock()
	srv.handlers[reqKind] = handler
	srv.mu.Unlock()
}

func (srv *Server) Address() string {
	return srv.ln.Addr().String()
}

func (srv *Server) Start() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.ln != nil {
		panic("server already started")
	}
	ln, err := net.Listen("tcp4", "")
	if err != nil {
		panic(fmt.Sprint("cannot start server: %s", err))
	}
	srv.ln = ln

	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleClient(client)
		}
	}()
}

func (srv *Server) Close() {
	srv.mu.Lock()
	srv.ln.Close()
	srv.mu.Unlock()
}

func (srv *Server) handleClient(c net.Conn) {
	for {
		kind, b, err := proto.ReadReq(c)
		if err != nil {
			return
		}
		srv.mu.RLock()
		fn, ok := srv.handlers[kind]
		if !ok {
			fn, ok = srv.handlers[AnyRequest]
		}
		srv.mu.RUnlock()

		if !ok {
			panic(fmt.Sprintf("no handler for %d", kind))
		}

		var request Serializable

		switch kind {
		case FetchRequest:
			request, err = proto.ReadFetchReq(bytes.NewBuffer(b))
		case ProduceRequest:
			request, err = proto.ReadProduceReq(bytes.NewBuffer(b))
		case OffsetRequest:
			request, err = proto.ReadOffsetReq(bytes.NewBuffer(b))
		case MetadataRequest:
			request, err = proto.ReadMetadataReq(bytes.NewBuffer(b))
		case ConsumerMetadataRequest:
			request, err = proto.ReadConsumerMetadataReq(bytes.NewBuffer(b))
		case OffsetCommitRequest:
			request, err = proto.ReadOffsetCommitReq(bytes.NewBuffer(b))
		case OffsetFetchRequest:
			request, err = proto.ReadOffsetFetchReq(bytes.NewBuffer(b))
		}

		if err != nil {
			panic(fmt.Sprintf("could not read message %d: %s", kind, err))
		}

		response := fn(request)
		if response != nil {
			b, err := response.Bytes()
			if err != nil {
				panic(fmt.Sprintf("cannot serialize %T: %s", response, err))
			}
			c.Write(b)
		}
	}
}

func (srv *Server) defaultRequestHandler(request Serializable) Serializable {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	switch req := request.(type) {
	case *proto.FetchReq:
		// TODO(husio) support
		// * max wait time
		// * max bytes
		// * min bytes
		resp := &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics:        make([]proto.FetchRespTopic, len(req.Topics)),
		}
		for ti, topic := range req.Topics {
			resp.Topics[ti] = proto.FetchRespTopic{
				Name:       topic.Name,
				Partitions: make([]proto.FetchRespPartition, len(topic.Partitions)),
			}
			for pi, part := range topic.Partitions {
				messages, err := srv.Logs.Messages(topic.Name, part.ID, part.FetchOffset, 10)
				resp.Topics[ti].Partitions[pi] = proto.FetchRespPartition{
					ID:        part.ID,
					Err:       err,
					TipOffset: srv.Logs.PartitionOffset(topic.Name, part.ID),
					Messages:  messages,
				}
			}
		}
		return resp
	case *proto.ProduceReq:
		resp := &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
		}
		resp.Topics = make([]proto.ProduceRespTopic, len(req.Topics))
		for ti, topic := range req.Topics {
			resp.Topics[ti] = proto.ProduceRespTopic{
				Name:       topic.Name,
				Partitions: make([]proto.ProduceRespPartition, len(topic.Partitions)),
			}
			for pi, part := range topic.Partitions {
				offset, err := srv.Logs.AddMessages(topic.Name, part.ID, part.Messages)
				resp.Topics[ti].Partitions[pi] = proto.ProduceRespPartition{
					ID:     part.ID,
					Err:    err,
					Offset: offset,
				}
			}

		}
		return resp
	case *proto.OffsetReq:
		panic("not implemented")
		return &proto.OffsetResp{
			CorrelationID: req.CorrelationID,
		}
	case *proto.MetadataReq:
		host, sport, err := net.SplitHostPort(srv.ln.Addr().String())
		if err != nil {
			panic(fmt.Sprintf("cannot split server address: %s", err))
		}
		port, err := strconv.Atoi(sport)
		if err != nil {
			panic(fmt.Sprintf("port '%s' is not a number: %s", sport, err))
		}
		if host == "" {
			host = "localhost"
		}

		topics := make([]proto.MetadataRespTopic, len(srv.Logs.logs))
		ti := 0
		for topic, partitions := range srv.Logs.logs {
			parts := make([]proto.MetadataRespPartition, len(partitions))
			pi := 0
			for part := range partitions {
				parts[pi] = proto.MetadataRespPartition{
					ID:       part,
					Leader:   1,
					Replicas: []int32{1},
					Isrs:     []int32{1},
				}
				pi += 1
			}
			topics[ti] = proto.MetadataRespTopic{
				Name:       topic,
				Partitions: parts,
			}
			ti += 1
		}
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				proto.MetadataRespBroker{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: topics,
		}
	case *proto.ConsumerMetadataReq:
		panic("not implemented")
		return &proto.ConsumerMetadataResp{
			CorrelationID: req.CorrelationID,
		}
	case *proto.OffsetCommitReq:
		panic("not implemented")
		return &proto.OffsetCommitResp{
			CorrelationID: req.CorrelationID,
		}
	case *proto.OffsetFetchReq:
		panic("not implemented")
		return &proto.OffsetFetchReq{
			CorrelationID: req.CorrelationID,
		}
	default:
		panic(fmt.Sprintf("unknown message type: %T", req))
	}
	return nil
}
