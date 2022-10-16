package worker

import "math"

type membershipInfo struct {
	topics []string
	data   map[string]map[string]bool
}

func (mi *membershipInfo) addMember(member string) {

	if mi.data == nil {
		mi.data = make(map[string]map[string]bool)
	}
	mi.data[member] = map[string]bool{}

	// If this is the only member assign all the topics to it
	if mi.data == nil || len(mi.data) == 0 {
		for _, topic := range mi.topics {
			mi.data[member][topic] = true
		}
		return
	}

	// If more members than topics then no need to assign members to topic
	tpm := mi.tpm(true)
	if tpm == 0 {
		return
	}

	// Start iterating over the members
	for key, topicMap := range mi.data {
		// Ignore if key is current member
		if key == member {
			continue
		}

		// IF more topics assigned to it then topics per member
		if len(topicMap) > tpm {
			off := len(topicMap) - tpm
			i := 0
			nkeys := make([]string, off)
			for topic, _ := range topicMap {
				nkeys[i] = topic
				i++
				if i == off {
					break
				}
			}

			for _, nkey := range nkeys {
				delete(topicMap, nkey)
				mi.data[member][nkey] = true
			}
		}
	}
}

func (mi *membershipInfo) removeMember(member string) {
	orphanMap, ok := mi.data[member]
	if !ok {
		// If node is not yet added to membership and removed do nothing
		return
	}
	delete(mi.data, member)

	// Capture all the orphaned topics in an array, we will need to assign only these topics
	// This to done ti minimize disruption
	orphanTopics := make([]string, len(orphanMap))
	i := 0
	for topic, _ := range orphanMap {
		orphanTopics[i] = topic
		i++
	}

	j := 0
	tpm := mi.tpm(false)
	for _, topicMap := range mi.data {
		j++
		// If more topics are assigned then topic per member or if it is the last member assign partitions
		if len(topicMap) < tpm || j == len(mi.data) {
			off := tpm - len(topicMap)
			if off > len(orphanTopics) {
				off = len(orphanTopics)
			}

			for k := 0; k < off; k++ {
				topicMap[orphanTopics[k]] = true
			}

			orphanTopics = orphanTopics[off:]
		}

		if len(orphanTopics) == 0 {
			break
		}
	}
}

func (mi *membershipInfo) tpm(add bool) int {
	mc := float64(len(mi.data))
	if add {
		mc += 1
	} else {
		mc -= 1
	}

	tc := float64(len(mi.topics))
	return int(math.Round(tc / mc))
}

func (mi *membershipInfo) getTopics(id string) []string {
	tmap := mi.data[id]
	topics := make([]string, len(tmap))
	i := 0

	for topic, _ := range tmap {
		topics[i] = topic
		i++
	}
	return topics
}
