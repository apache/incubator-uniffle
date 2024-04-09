package util

import (
	"testing"
)

func TestGetLastAppNum(t *testing.T) {
	jsonString := `
		{
			"metrics": [{
				"name": "app_num_with_node",
				"labelNames": ["tags"],
				"labelValues": ["ss_v5,GRPC"],
				"value": 10.0,
				"timestampMs": null
			}, {
				"name": "total_remove_resource_time",
				"labelNames": ["quantile"],
				"labelValues": ["0.5"],
				"value": "NaN",
				"timestampMs": null
			}],
			"timeStamp": 1712575271639
		}
	`
	if num, err := getLastAppNum([]byte(jsonString)); err != nil {
		t.Fatal("getLastAppNum failed, casued by ", err.Error())
	} else if num != 10 {
		t.Fatal("Get wrong app number: ", num)
	}
}
