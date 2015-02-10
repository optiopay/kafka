package kafka

import (
	"fmt"
)

var errnoToErr = map[int16]error{
	-1: &KafkaError{-1, "unknown"},
	0:  &KafkaError{0, "no error"},
	1:  &KafkaError{1, "offset out of range"},
	2:  &KafkaError{2, "invalid message"},
	3:  &KafkaError{3, "unknown topic or partition"},
	4:  &KafkaError{4, "invalid message size"},
	5:  &KafkaError{5, "leader not available"},
	6:  &KafkaError{6, "no leader for partition"},
	7:  &KafkaError{7, "request timeed out"},
	8:  &KafkaError{8, "broker not available"},
	9:  &KafkaError{9, "replica not available"},
	10: &KafkaError{10, "message size too large"},
	11: &KafkaError{11, "scale controller epoch"},
	12: &KafkaError{12, "ofset metadata too large"},
	14: &KafkaError{14, "offsets load in progress"},
	15: &KafkaError{15, "consumer coordinator not available"},
	16: &KafkaError{16, "not coordinator for consumer"},
}

type KafkaError struct {
	errno   int16
	message string
}

func (err *KafkaError) Error() string {
	return fmt.Sprintf("%s (%d)", err.message, err.errno)
}

func (err *KafkaError) Errno() int {
	return int(err.errno)
}

func errFromNo(errno int16) error {
	if errno == 0 {
		return nil
	}
	err, ok := errnoToErr[errno]
	if !ok {
		return fmt.Errorf("unknown kafka error %d", errno)
	}
	return err
}
