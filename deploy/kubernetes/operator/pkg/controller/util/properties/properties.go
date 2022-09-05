package properties

import (
	"sort"
	"strings"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/constants"
)

// UpdateProperties updates configuration of coordinators or shuffle servers.
func UpdateProperties(properties string, kv map[constants.PropertyKey]string) string {
	oldKv := parseProperties(properties)
	for k, v := range kv {
		oldKv[k] = v
	}
	return generateProperties(oldKv)
}

// parseProperties converts string in properties format used in Java to a map.
func parseProperties(properties string) map[constants.PropertyKey]string {
	result := make(map[constants.PropertyKey]string)
	lines := strings.Split(properties, "\n")
	for _, line := range lines {
		kv := strings.Split(line, " ")
		if len(kv) < 2 {
			continue
		}
		result[constants.PropertyKey(kv[0])] = kv[1]
	}
	return result
}

// generateProperties converts a map to string in properties format used in Java.
func generateProperties(kv map[constants.PropertyKey]string) string {
	var lines []string
	for k, v := range kv {
		lines = append(lines, string(k)+" "+v)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
