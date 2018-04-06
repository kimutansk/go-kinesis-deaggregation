package deaggregation

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/kimutansk/go-kinesis-deaggregation/pb"
)

// IsAggregatedRecord judges "NotAggregatedRecord" is not AggregatedRecord.
func Test_IsAggregatedRecord_JudgeNonAggregatedRecord(t *testing.T) {
	target := "NotAggregatedRecord"
	targetByteArray := []byte(target)

	actual := IsAggregatedRecord(targetByteArray)
	if actual == false {
		t.Log("NotAggregatedRecord judge succeed.")
	} else {
		t.Fail()
	}
}

// IsAggregatedRecord judges MinimumAggregatedRecord is AggregatedRecord.
func Test_IsAggregatedRecord_JudgeMinimumAggregatedRecord(t *testing.T) {

	targetRecord := &pb.Record{
		PartitionKeyIndex: proto.Uint64(1111),
		Data:              []byte("MinimumAggregatedRecord"),
	}

	targetAggregatedRecord := &pb.AggregatedRecord{
		Records: []*pb.Record{targetRecord},
	}

	targetBytes, err := proto.Marshal(targetAggregatedRecord)
	if err != nil {
		fmt.Print("unmarshaling error: ", err)
		t.Fail()
	}

	md5Hash := md5.New()
	md5Hash.Write(targetBytes)
	checkSum := md5Hash.Sum(nil)
	target := append(magicNumber, targetBytes...)
	target = append(target, checkSum...)

	actual := IsAggregatedRecord(target)
	if actual == true {
		t.Log("MinimumAggregatedRecord judge succeed.")
	} else {
		t.Fail()
	}
}

// IsAggregatedRecord judges FullAggregatedRecord is AggregatedRecord.
func Test_IsAggregatedRecord_JudgeFullAggregatedRecord(t *testing.T) {

	tag1 := &pb.Tag{
		Key:   proto.String("TagKey1"),
		Value: proto.String("TagValue1"),
	}

	tag2 := &pb.Tag{
		Key:   proto.String("TagKey2"),
		Value: proto.String("TagValue2"),
	}

	targetRecord1 := &pb.Record{
		PartitionKeyIndex:    proto.Uint64(1111),
		ExplicitHashKeyIndex: proto.Uint64(11111111),
		Data:                 []byte("FullAggregatedRecord1"),
		Tags:                 []*pb.Tag{tag1, tag2},
	}

	targetRecord2 := &pb.Record{
		PartitionKeyIndex:    proto.Uint64(2222),
		ExplicitHashKeyIndex: proto.Uint64(22222222),
		Data:                 []byte("FullAggregatedRecord2"),
		Tags:                 []*pb.Tag{tag2, tag1},
	}

	targetAggregatedRecord := &pb.AggregatedRecord{
		PartitionKeyTable:    []string{"Table1", "Table2"},
		ExplicitHashKeyTable: []string{"KeyTable1", "KeyTable2"},
		Records:              []*pb.Record{targetRecord1, targetRecord2},
	}

	targetBytes, err := proto.Marshal(targetAggregatedRecord)
	if err != nil {
		fmt.Print("unmarshaling error: ", err)
		t.Fail()
	}

	md5Hash := md5.New()
	md5Hash.Write(targetBytes)
	checkSum := md5Hash.Sum(nil)
	target := append(magicNumber, targetBytes...)
	target = append(target, checkSum...)

	actual := IsAggregatedRecord(target)
	if actual == true {
		t.Log("MinimumAggregatedRecord judge succeed.")
	} else {
		t.Fail()
	}
}
