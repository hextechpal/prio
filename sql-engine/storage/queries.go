package storage

import "errors"

var (
	ErrorWrongKey          = errors.New("query not present. wrong key")
	ErrorUnsupportedDriver = errors.New("driver not supported")
)

var mysql = map[string]string{
	"addTopic": `INSERT INTO topics(name, description, created_at, updated_at) VALUES (?, ?, ?, ?)`,
	"addJob":   `INSERT INTO jobs(topic, payload, priority, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,

	"topJob":   `SELECT jobs.id, jobs.payload from jobs where jobs.topic = ? AND jobs.status = ? ORDER BY priority DESC, updated_at ASC LIMIT 1 FOR UPDATE`,
	"claimJob": `UPDATE jobs SET status = ?, claimed_at = ?, claimed_by = ?  WHERE jobs.id = ?`,

	"jobById":     `SELECT jobs.id, jobs.status from jobs where jobs.id = ? AND jobs.topic = ? FOR UPDATE `,
	"completeJob": `UPDATE jobs SET status = ?, completed_at = ? WHERE jobs.id = ?`,
}

type QueryMap struct {
	driver string
}

func (qm *QueryMap) addTopic() string {
	q, _ := qm.getQuery("addTopic")
	return q
}

func (qm *QueryMap) addJob() string {
	q, _ := qm.getQuery("addJob")
	return q
}

func (qm *QueryMap) topJob() string {
	q, _ := qm.getQuery("topJob")
	return q
}

func (qm *QueryMap) claimJob() string {
	q, _ := qm.getQuery("claimJob")
	return q
}

func (qm *QueryMap) jobById() string {
	q, _ := qm.getQuery("jobById")
	return q
}

func (qm *QueryMap) completeJob() string {
	q, _ := qm.getQuery("completeJob")
	return q
}

func (qm *QueryMap) getQuery(query string) (string, error) {
	switch qm.driver {
	case "mysql":
		q, ok := mysql[query]
		if !ok {
			return "", ErrorWrongKey
		}
		return q, nil
	default:
		return "", ErrorUnsupportedDriver
	}
}
