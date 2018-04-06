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
	targetRecord := createMinimumAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	target := append(magicNumber, targetRecord...)
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
	targetRecord := createFullAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	actual := IsAggregatedRecord(targetBytes)
	if actual == true {
		t.Log("MinimumAggregatedRecord judge succeed.")
	} else {
		t.Fail()
	}
}

func Test_ExtractRecordDatas_MinimumAggregatedRecord(t *testing.T) {
	targetRecord := createMinimumAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	actual, _ := ExtractRecordDatas(targetBytes)

	if len(actual) == 1 && string(actual[0]) == "MinimumAggregatedRecord" {
		t.Log("MinimumAggregatedRecord extract succeed.")
	} else {
		t.Fail()
	}
}

func Test_ExtractRecordDatas_FullAggregatedRecord(t *testing.T) {
	targetRecord := createFullAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	actual, _ := ExtractRecordDatas(targetBytes)

	if len(actual) == 2 && string(actual[0]) == "FullAggregatedRecord1" && string(actual[1]) == "FullAggregatedRecord2" {
		t.Log("FullAggregatedRecord extract succeed.")
	} else {
		t.Fail()
	}
}

func Test_Unmarshal_MinimumAggregatedRecord(t *testing.T) {
	targetRecord := createMinimumAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	actual, _ := Unmarshal(targetBytes)

	if len(actual.GetExplicitHashKeyTable()) == 0 && len(actual.GetPartitionKeyTable()) == 0 && len(actual.GetRecords()) == 1 {
		t.Log("Record extract succeed.")
	} else {
		t.Fail()
	}
	actualRecord := actual.GetRecords()[0]
	if actualRecord.GetPartitionKeyIndex() == uint64(1111) && actualRecord.GetExplicitHashKeyIndex() == uint64(0) && string(actualRecord.GetData()) == "MinimumAggregatedRecord" && len(actualRecord.GetTags()) == 0 {
		t.Log("Record check succeed.")
	} else {
		t.Fail()
	}
}

func Test_Unmarshal_FullAggregatedRecord(t *testing.T) {
	targetRecord := createFullAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	actual, _ := Unmarshal(targetBytes)

	if len(actual.GetExplicitHashKeyTable()) == 2 && len(actual.GetPartitionKeyTable()) == 2 && len(actual.GetRecords()) == 2 {
		t.Log("Record extract succeed.")
	} else {
		t.Fail()
	}
	if actual.GetPartitionKeyTable()[0] == "Table1" && actual.GetPartitionKeyTable()[1] == "Table2" && actual.GetExplicitHashKeyTable()[0] == "KeyTable1" && actual.GetExplicitHashKeyTable()[1] == "KeyTable2" {
		t.Log("Parent record check succeed.")
	} else {
		t.Fail()
	}

	actualRecord1 := actual.GetRecords()[0]
	actualRecord2 := actual.GetRecords()[1]
	if actualRecord1.GetPartitionKeyIndex() == uint64(1111) && actualRecord1.GetExplicitHashKeyIndex() == uint64(11111111) && string(actualRecord1.GetData()) == "FullAggregatedRecord1" && len(actualRecord1.GetTags()) == 2 {
		t.Log("Record1 check succeed.")
	} else {
		t.Fail()
	}
	if actualRecord2.GetPartitionKeyIndex() == uint64(2222) && actualRecord2.GetExplicitHashKeyIndex() == uint64(22222222) && string(actualRecord2.GetData()) == "FullAggregatedRecord2" && len(actualRecord2.GetTags()) == 2 {
		t.Log("Record2 check succeed.")
	} else {
		t.Fail()
	}

	actualRecord1Tags := actualRecord1.GetTags()
	actualRecord2Tags := actualRecord2.GetTags()
	if actualRecord1Tags[0].GetKey() == "TagKey1" && actualRecord1Tags[0].GetValue() == "TagValue1" && actualRecord1Tags[1].GetKey() == "TagKey2" && actualRecord1Tags[1].GetValue() == "TagValue2" {
		t.Log("Record1Tag check succeed.")
	} else {
		t.Fail()
	}
	if actualRecord2Tags[0].GetKey() == "TagKey2" && actualRecord2Tags[0].GetValue() == "TagValue2" && actualRecord2Tags[1].GetKey() == "TagKey1" && actualRecord2Tags[1].GetValue() == "TagValue1" {
		t.Log("Record1Tag check succeed.")
	} else {
		t.Fail()
	}
}

func createMinimumAggregateRecordMarshaledBytes() []byte {
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
		panic(err)
	}

	return targetBytes
}

func createFullAggregateRecordMarshaledBytes() []byte {
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
		panic(err)
	}

	return targetBytes
}
