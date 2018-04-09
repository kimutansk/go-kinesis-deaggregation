package deaggregation

import (
	"crypto/md5"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/kimutansk/go-kinesis-deaggregation/pb"
)

// IsAggregatedRecord judges "NotAggregatedRecord" is not AggregatedRecord.
func Test_IsAggregatedRecord_JudgeNonAggregatedRecord(t *testing.T) {
	target := "NotAggregatedRecord"
	targetByteArray := []byte(target)

	actual := IsAggregatedRecord(targetByteArray)
	if actual != false {
		t.Errorf("IsAggregatedRecord(\"%v\") want %v but %v.", target, false, actual)
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
	if actual != true {
		t.Errorf("IsAggregatedRecord(MinimumAggregatedRecord) want %v but %v.", true, actual)
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
	if actual != true {
		t.Errorf("IsAggregatedRecord(FullAggregatedRecord) want %v but %v.", true, actual)
	}
}

func Test_ExtractRecordDatas_MinimumAggregatedRecord(t *testing.T) {
	targetRecord := createMinimumAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	expected := [][]byte{[]byte("MinimumAggregatedRecord")}
	actual, _ := ExtractRecordDatas(targetBytes)

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ExtractRecordDatas(MinimumAggregatedRecord) want %v but %v.", expected, actual)
	}
}

func Test_ExtractRecordDatas_FullAggregatedRecord(t *testing.T) {
	targetRecord := createFullAggregateRecordMarshaledBytes()

	md5Hash := md5.New()
	md5Hash.Write(targetRecord)
	checkSum := md5Hash.Sum(nil)
	targetBytes := append(magicNumber, targetRecord...)
	targetBytes = append(targetBytes, checkSum...)

	expected := [][]byte{[]byte("FullAggregatedRecord1"), []byte("FullAggregatedRecord2")}
	actual, _ := ExtractRecordDatas(targetBytes)

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ExtractRecordDatas(FullAggregatedRecord) want %v but %v.", expected, actual)
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

	if len(actual.GetExplicitHashKeyTable()) != 0 {
		t.Errorf("Unmarshal(MinimumAggregatedRecord)'s ExplicitHashKeyTable want %v results but %v results.", 0, actual.GetExplicitHashKeyTable())
	}
	if len(actual.GetPartitionKeyTable()) != 0 {
		t.Errorf("Unmarshal(MinimumAggregatedRecord)'s PartitionKeyTable want %v results but %v results.", 0, actual.GetPartitionKeyTable())
	}
	if len(actual.GetRecords()) != 1 {
		t.Errorf("Unmarshal(MinimumAggregatedRecord)'s Records want %v records but %v records.", 1, len(actual.GetRecords()))
	}

	actualRecord := actual.GetRecords()[0]
	if actualRecord.GetPartitionKeyIndex() != uint64(1111) {
		t.Errorf("PartitionKeyIndex want %v but %v.", uint64(1111), actualRecord.GetPartitionKeyIndex())
	}
	if actualRecord.GetExplicitHashKeyIndex() != uint64(0) {
		t.Errorf("ExplicitHashKeyIndex want %v but %v.", uint64(0), actualRecord.GetExplicitHashKeyIndex())
	}
	if len(actualRecord.GetTags()) != 0 {
		t.Errorf("Tags want %v tags but %v tags.", 0, len(actualRecord.GetTags()))
	}
	expected := "MinimumAggregatedRecord"
	if string(actualRecord.GetData()) != expected {
		t.Errorf("Data want %v but %v.", expected, string(actualRecord.GetData()))
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

	expectedPartitionKeyTable := []string{"Table1", "Table2"}
	if !reflect.DeepEqual(expectedPartitionKeyTable, actual.GetPartitionKeyTable()) {
		t.Errorf("Unmarshal(FullAggregatedRecord)'s PartitionKeyTable want %v but %v.", expectedPartitionKeyTable, actual.GetPartitionKeyTable())
	}
	expectedExplicitHashKeyTable := []string{"KeyTable1", "KeyTable2"}
	if !reflect.DeepEqual(expectedExplicitHashKeyTable, actual.GetExplicitHashKeyTable()) {
		t.Errorf("Unmarshal(FullAggregatedRecord)'s ExplicitHashKeyTable want %v but %v.", expectedExplicitHashKeyTable, actual.GetExplicitHashKeyTable())
	}
	if len(actual.GetRecords()) != 2 {
		t.Errorf("Unmarshal(FullAggregatedRecord)'s Records want %v records but %v records.", 2, len(actual.GetRecords()))
	}

	actualRecord1 := actual.GetRecords()[0]
	actualRecord2 := actual.GetRecords()[1]
	if actualRecord1.GetPartitionKeyIndex() != uint64(1111) || actualRecord1.GetExplicitHashKeyIndex() != uint64(11111111) || string(actualRecord1.GetData()) != "FullAggregatedRecord1" || len(actualRecord1.GetTags()) != 2 {
		t.Error("Record1 check failed.")
	}
	if actualRecord2.GetPartitionKeyIndex() != uint64(2222) || actualRecord2.GetExplicitHashKeyIndex() != uint64(22222222) || string(actualRecord2.GetData()) != "FullAggregatedRecord2" || len(actualRecord2.GetTags()) != 2 {
		t.Error("Record2 check failed.")
	}

	actualRecord1Tags := actualRecord1.GetTags()
	actualRecord2Tags := actualRecord2.GetTags()
	if actualRecord1Tags[0].GetKey() != "TagKey1" || actualRecord1Tags[0].GetValue() != "TagValue1" || actualRecord1Tags[1].GetKey() != "TagKey2" || actualRecord1Tags[1].GetValue() != "TagValue2" {
		t.Error("Record1Tag check failed.")
	}
	if actualRecord2Tags[0].GetKey() != "TagKey2" || actualRecord2Tags[0].GetValue() != "TagValue2" || actualRecord2Tags[1].GetKey() != "TagKey1" || actualRecord2Tags[1].GetValue() != "TagValue1" {
		t.Error("Record2Tag check failed.")
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
