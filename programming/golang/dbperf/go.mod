module tools/dbperf

go 1.13

require (
	github.com/gocql/gocql v0.0.0-20210310062040-27ecec822885 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/gookit/color v1.3.8 // indirect
	github.com/scylladb/gocqlx/v2 v2.3.0 // indirect
	gopkg.in/rethinkdb/rethinkdb-go.v6 v6.2.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v0.0.0-20210215130051-390af0fb2915
