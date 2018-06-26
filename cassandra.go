package gdsb

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

//CassandraConfig is the cassandra configuration object
type CassandraConfig struct {
	Hosts    string
	Port     int
	Username string
	Password string
	Keyspace string
	Timeout  int
}

// CassandraSession holds cassandra sessions
var CassandraSession *gocql.Session

//CassandraSessionInit will load cassandra session
func CassandraSessionInit(c CassandraConfig) error {
	if len(c.Hosts) == 0 || len(c.Username) == 0 || len(c.Password) == 0 || len(c.Keyspace) == 0 {
		e := fmt.Sprint("Cannot create Cassandra session. Configuration missing. Configuration provided: ", c)
		return errors.New(e)
	}
	cs, err := CreateCassandraConnection(c)
	CassandraSession = cs
	return err
}

//CreateCassandraConnection creates cassandra session
func CreateCassandraConnection(c CassandraConfig) (*gocql.Session, error) {
	cluster := gocql.NewCluster(c.Hosts)
	cluster.Keyspace = c.Keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c.Username,
		Password: c.Password,
	}
	if c.Timeout == 0 {
		c.Timeout = 10
	}
	cluster.Timeout = time.Duration(c.Timeout) * time.Second
	return cluster.CreateSession()
}

//CassandraQuery holds the query to be run
type CassandraQuery struct {
	Querry string
}

//CQUpsert will execute the CassandraQuery passed
func CQUpsert(c CassandraQuery) error {
	log.Println("Executing query: ", c.Querry)
	return CassandraSession.Query(c.Querry).Exec()
}