package deaggregation

import (
	"crypto/md5"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/kimutansk/go-kinesis-deaggregation/pb"
)

const digestLength int32 = 16

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

// IsAggregatedRecord judges whether input message is Kinesis Aggregated Record or not.
func IsAggregatedRecord(target []byte) bool {
	length := int32(len(target))
	if length < digestLength {
		return false
	}

	if !reflect.DeepEqual(magicNumber, target[0:len(magicNumber)]) {
		return false
	}

	md5Hash := md5.New()
	checkSum := md5Hash.Sum(target[len(magicNumber) : length-digestLength-int32(len(magicNumber))])

	if !reflect.DeepEqual(target[length-digestLength:digestLength], checkSum) {
		return false
	}

	return true
}

// ExtractRecordDatas extracts Record.Data slice from Kinesis Aggregated Record.
func ExtractRecordDatas(target []byte) ([][]byte, error) {
	length := int32(len(target))
	aggregated := &pb.AggregatedRecord{}

	if err := proto.Unmarshal(target[len(magicNumber):length-digestLength-int32(len(magicNumber))], aggregated); err != nil {
		return nil, err
	}

	records := aggregated.GetRecords()
	recordNum := len(records)
	recordDatas := [][]byte{{}}
	for index := 0; index < recordNum; index++ {
		recordDatas[index] = records[index].GetData()[:]
	}

	return recordDatas, nil
}
