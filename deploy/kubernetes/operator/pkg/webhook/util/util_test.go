/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"encoding/json"
	"math"
	"testing"
)

// JSONFloatWrapper is struct which contain JSONFloat
type JSONFloatWrapper struct {
	Value1 JSONFloat `json:"value1"`
	Value2 JSONFloat `json:"value2"`
}

func TestParseJsonFloat(t *testing.T) {
	wrapper := JSONFloatWrapper{Value1: JSONFloat(math.NaN()), Value2: 9.99}
	bytes, err := json.Marshal(wrapper)
	if err != nil {
		t.Fatal("Marshal JsonFloat failed, caused by ", err.Error())
	}
	parsed := &JSONFloatWrapper{}
	if err := json.Unmarshal(bytes, parsed); err != nil {
		t.Fatal("Unmarshal JsonFloat failed, caused by ", err.Error())
	} else if !math.IsNaN(float64(parsed.Value1)) {
		t.Fatal("Value1 should be Nan")
	} else if math.Abs(float64(parsed.Value2)-9.99) > 1e-9 {
		t.Fatal("Value1 should be 9.99")
	}
}

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
