package deaggregation

import (
	"crypto/md5"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/kimutansk/go-kinesis-deaggregation/pb"
)

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

// IsAggregatedRecord judges whether input message is Kinesis Aggregated Record or not.
func IsAggregatedRecord(target []byte) bool {
	length := int32(len(target))
	if length < md5.Size {
		return false
	}

	if !reflect.DeepEqual(magicNumber, target[0:len(magicNumber)]) {
		return false
	}

	md5Hash := md5.New()
	md5Hash.Write(target[len(magicNumber) : length-md5.Size])
	checkSum := md5Hash.Sum(nil)

	if !reflect.DeepEqual(target[length-md5.Size:length], checkSum) {
		return false
	}

	return true
}

// ExtractRecordDatas extracts Record.Data slice from Kinesis Aggregated Record.
func ExtractRecordDatas(target []byte) ([][]byte, error) {
	length := int32(len(target))
	aggregated := &pb.AggregatedRecord{}

	if err := proto.Unmarshal(target[len(magicNumber):length-md5.Size], aggregated); err != nil {
		return nil, err
	}

	records := aggregated.GetRecords()
	recordDatas := [][]byte{}
	for index := 0; index < len(records); index++ {
		recordDatas = append(recordDatas, records[index].GetData())
	}

	return recordDatas, nil
}

// Unmarshal extracts AggregatedRecord from Kinesis Aggregated Record.
func Unmarshal(target []byte) (*pb.AggregatedRecord, error) {
	length := int32(len(target))
	aggregated := &pb.AggregatedRecord{}

	if err := proto.Unmarshal(target[len(magicNumber):length-md5.Size], aggregated); err != nil {
		return nil, err
	}

	return aggregated, nil
}
