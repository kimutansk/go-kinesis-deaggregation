# go-kinesis-deaggregation [![CircleCI][circleci-image]][circleci-status] [![codecov][codecov-image]][codecov-status]

A deaggregation module for Amazon Kinesis Data Streams's Aggregated Record. 
This deaggregation module is based on [Aggregation format][aggregation-format].

### Reference links

- [Aggregation format][aggregation-format]
- [Considerations When Using KPL Aggregation][kpl-aggregation]
- [Consumer De-aggregation][de-aggregation]

### Example

```go
package main

import (
    "github.com/kimutansk/go-kinesis-deaggregation"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewDevelopment()
    sugar = logger.Sugar()

    aggregatedRecordBytes := // Get Aggregated Record by []byte
    recordDatas, err := deaggregation.ExtractRecordDatas(dataBytes)

    if err != nil {
        sugar.Warnw("Failed deserialize Received Bytes.", "AggregatedRecordBytes", aggregatedRecordBytes)
        panic(err)
    }

    recordTexts := []string{}
    for _, recordData := range recordDatas {
        recordTexts = append(recordTexts, string(recordData))
    }

    sugar.Infow("Succeed deserialize Received Bytes.", "RecordTexts", strings.Join(recordTexts[:], ","))
}
```

### License

- License: Apache License, Version 2.0

[circleci-image]: https://circleci.com/gh/kimutansk/go-kinesis-deaggregation/tree/master.svg?style=svg
[circleci-status]: https://circleci.com/gh/kimutansk/go-kinesis-deaggregation

[codecov-image]: https://codecov.io/gh/kimutansk/go-kinesis-deaggregation/branch/master/graph/badge.svg
[codecov-status]: https://circleci.com/gh/kimutansk/go-kinesis-deaggregation

[aggregation-format]: https://github.com/a8m/kinesis-producer/blob/master/aggregation-format.md
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
