package kafka

import (
	"fmt"
)

const (
	reqProduce      = 0
	reqFetch        = 1
	reqOffset       = 2
	reqMetadata     = 3
	reqOffsetCommit = 8
	reqOffsetFetch  = 9
	reqConsumerMeta = 10

	compressNone   = 0
	compressGZIP   = 1
	compressSnappy = 2

	offsetLatest   = -1
	offsetEarliest = -2
)

var apiErrors = map[int16]error{
	-1: &APIError{-1, "unknown"},
	0:  &APIError{0, "no error"},
	1:  &APIError{1, "offset out of range"},
	2:  &APIError{2, "invalid message"},
	3:  &APIError{3, "unknown topic or partition"},
	4:  &APIError{4, "invalid message size"},
	5:  &APIError{5, "leader not available"},
	6:  &APIError{6, "no leader for partition"},
	7:  &APIError{7, "request timeed out"},
	8:  &APIError{8, "broker not available"},
	9:  &APIError{9, "replica not available"},
	10: &APIError{10, "message size too large"},
	11: &APIError{11, "scale controller epoch"},
	12: &APIError{12, "ofset metadata too large"},
	14: &APIError{14, "offsets load in progress"},
	15: &APIError{15, "consumer coordinator not available"},
	16: &APIError{16, "not coordinator for consumer"},
}

// APIError represents an error from the Kafka API.
type APIError struct {
	errno   int16
	message string
}

// Error returns a printable error string.
func (err *APIError) Error() string {
	return fmt.Sprintf("%s (%d)", err.message, err.errno)
}

// errFromNo returns the error for a specific errno.
func errFromNo(errno int16) error {
	if errno == 0 {
		return nil
	}
	err, ok := apiErrors[errno]
	if !ok {
		return fmt.Errorf("unknown kafka error %d", errno)
	}
	return err
}
