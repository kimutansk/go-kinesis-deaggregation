# go-kinesis-deaggregation [![License][license-image]][license-url][![CircleCI][circleci-image]][circleci-build-status]

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
MIT

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
[license-url]: LICENSE

[circleci-image]: https://circleci.com/gh/kimutansk/go-kinesis-deaggregation.svg?style=svg
[circleci-build-status]: https://circleci.com/gh/kimutansk/go-kinesis-deaggregation

[aggregation-format]: https://github.com/a8m/kinesis-producer/blob/master/aggregation-format.md
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
