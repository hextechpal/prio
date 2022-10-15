package worker

import "math"

type membershipInfo struct {
	topics []string
	data   map[string]map[string]bool
}

func (mi *membershipInfo) addMember(member string) {
	tpn := int(math.Round(float64(len(mi.topics)) / float64(len(mi.data)+1)))
	if tpn == 0 {
		return
	}

	if mi.data == nil || len(mi.data) == 0 {
		valMap := make(map[string]bool)
		for _, topic := range mi.topics {
			valMap[topic] = true
		}
		mi.data = map[string]map[string]bool{member: valMap}
		return
	}

	mi.data[member] = make(map[string]bool)
	for key, topicMap := range mi.data {
		if key == member {
			continue
		}

		if len(topicMap) > tpn {
			off := len(topicMap) - tpn
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
		// TODO handle if the key is not present
		return
	}

	delete(mi.data, member)
	orphanTopics := make([]string, len(orphanMap))
	i := 0
	for topic, _ := range orphanMap {
		orphanTopics[i] = topic
		i++
	}

	tpn := int(math.Round(float64(len(mi.topics)) / float64(len(mi.data))))
	j := 0
	for _, topicMap := range mi.data {
		j++
		if len(topicMap) < tpn || j == len(mi.data) {
			off := tpn - len(topicMap)
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

func (mi *membershipInfo) topicCount() int {
	return len(mi.topics)
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
