package main

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/gookit/color"
	"math"
	"math/rand"
	"strings"
	"time"
)

type ScyllaQueries struct {
	insertGroupQuery        *gocql.Query
	insertGroupMemberQuery  *gocql.Query
	selectMembers			*gocql.Query
}

type ScyllaDatabase struct {
	cluster *gocql.ClusterConfig
	session *gocql.Session
	context context.Context
	queries []*ScyllaQueries
}

func (db *ScyllaDatabase) createDatabase() error {
	color.Notice.Println("creating database...")
	var err error
	if db.session, err = db.cluster.CreateSession(); err != nil {
		return err
	}

	if err = db.session.Query(`DROP TABLE IF EXISTS groups;`).WithContext(db.context).Exec(); err != nil {
		return err
	}
	if err = db.session.Query(`CREATE TABLE groups (id UUID, type TINYINT, group_level TINYINT, PRIMARY KEY (id));`).WithContext(db.context).Exec(); err != nil {
		return err
	}
	if err = db.session.Query(`DROP TABLE IF exists group_members;`).WithContext(db.context).Exec(); err != nil {
		return err
	}
	if err = db.session.Query(`CREATE TABLE group_members (group_id UUID, member_id UUID, member_type TINYINT, group_level TINYINT, PRIMARY KEY (group_id, member_id));`).WithContext(db.context).Exec(); err != nil {
		return err
	}

	sql := fmt.Sprintf("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : %d};", Config.DatabaseName, Config.DatabaseReplicationFactor)
	if err = db.session.Query(sql).WithContext(db.context).Exec(); err != nil {
		return err
	}


	return nil
}

func (db *ScyllaDatabase) insertGroup(workerIndex int, groupLevel int, groupNumber int) error {
	var groupId uuid.UUID
	var err error
	var groupType int
	groupId = customUUID(uint16(groupLevel), uint32(groupNumber + 1))

	if groupLevel == 1 {
		groupType = 1
	} else {
		groupType = 2
	}
	if err = db.queries[workerIndex].insertGroupQuery.Bind( gocql.UUID(groupId), groupType, groupLevel).WithContext(db.context).Exec(); err != nil {
		return err
	}

	return nil
}

func (db *ScyllaDatabase) insertGroupMember(workerIndex int, groupLevel int, groupNumber int, memberIndex int, subgroup bool) error {
	var groupId uuid.UUID
	var memberId uuid.UUID
	var err error
	var memberGroupLevel int = 1
	var memberNumber int = 1
	var totalGroupCountInLevel int
	groupId = customUUID(uint16(groupLevel), uint32(groupNumber + 1))

	if subgroup {
		memberGroupLevel = groupLevel - 1
		totalGroupCountInLevel = int(float64(Config.FirstLevelGroupCount) * math.Pow(Config.LevelGroupFactor, float64(groupLevel - 2)))
	} else {
		totalGroupCountInLevel = Config.FirstLevelGroupCount
	}

	memberNumber = rand.Intn(totalGroupCountInLevel)
//	memberNumber = memberIndex


	memberId = customUUID(uint16(memberGroupLevel), uint32(memberNumber + 1))

	var memberType int
	if !subgroup  {
		memberType = 1
	} else {
		memberType = 2
	}

	if err = db.queries[workerIndex].insertGroupMemberQuery.Bind(gocql.UUID(groupId), gocql.UUID(memberId), memberType, groupLevel).WithContext(db.context).Exec(); err != nil {
		return err
	}

	return nil
}

func getLastLevelGroups(uuids *[] uuid.UUID) {
	var count int = int(float64(Config.FirstLevelGroupCount) * math.Pow(Config.LevelGroupFactor, float64(Config.LevelCount - 1)))
	*uuids = make([]uuid.UUID, count)
	for i := 0; i < count; i++ {
		(*uuids)[i] = customUUID(uint16(Config.LevelCount), uint32(i + 1))
	}
}


func (db *ScyllaDatabase) selectMembers(workerIndex int, job *SelectMembersJob) error {
	var start time.Time = time.Now()
	for len(job.groups) > 0 {
		var err error
		var n int = len(job.groups)
		if n >= 100 {
			n = 100
		}
		var sqlParams string = strings.Repeat("?,", n*2)[:n*2-1]

		job.deep++

		query := db.session.Query(`SELECT member_id, member_type FROM group_members WHERE group_id IN (` + sqlParams + `);`)
//		query := db.queries[workerIndex].selectMembers

		query.SetConsistency(gocql.LocalOne)

		var params []interface{} = make([]interface{}, n)
		for i := 0; i < n; i++ {
			params[i] = gocql.UUID(job.groups[i])
		}
		job.groups = job.groups[n:]
		query.Bind(params...)

		var pageState []byte
		for {
			// We use PageSize(2) for the sake of example, use larger values in production (default is 5000) for performance
			// reasons.
			iter := query.PageSize(10000).PageState(pageState).Iter()
			nextPageState := iter.PageState()
			scanner := iter.Scanner()
			for scanner.Next() {
				var memberId gocql.UUID
				var memberType int

				if err = scanner.Scan(&memberId, &memberType); err != nil {
					return err
				}
				if memberType == 2 {
					job.groups = append(job.groups, uuid.UUID(memberId))
					job.subgroupCount++
				} else {
					job.members[uuid.UUID(memberId)] = ""
				}
			}

			if err = scanner.Err(); err != nil {
				return err
			}
			if len(nextPageState) == 0 {
				break
			}
			pageState = nextPageState
		}
	}
	job.duration = time.Now().Sub(start)

	return nil
}

func (db *ScyllaDatabase) SelectMembers() error {
	var helper DatabaseHelper
	helper.SelectMembers(db)
	return nil
}

func (db *ScyllaDatabase) FillData() error {
	helper := DatabaseHelper{}
	if err := helper.FillData(db); err != nil {
		return err
	}
	return nil
}

func (db *ScyllaDatabase) Initialize() error {

	var err error
	db.cluster = gocql.NewCluster(Config.DatabaseEndpoints...)
	db.cluster.Keyspace = Config.DatabaseName

	db.cluster.Consistency = gocql.One
	db.cluster.Timeout = Config.DatabaseTimeout * time.Millisecond
	db.cluster.ConnectTimeout = Config.DatabaseTimeout * time.Millisecond
	if db.session, err = db.cluster.CreateSession(); err != nil {
		return err
	}

	db.context = context.Background()
	if Config.DatabaseCreate == true {
		if err = db.createDatabase(); err != nil {
			return err
		}
	}

	db.queries = make([]*ScyllaQueries, Config.ParallelQueryCount)
	for workerIndex := 0; workerIndex < Config.ParallelQueryCount; workerIndex++ {

		queries := &ScyllaQueries{
			db.session.Query(`INSERT INTO groups (id, type, group_level) VALUES (?, ?, ?);`),
			db.session.Query(`INSERT INTO group_members (group_id, member_id, member_type, group_level) VALUES (?, ?, ?, ?);`),
			db.session.Query(`SELECT member_id, member_type FROM group_members WHERE group_id IN (?);`),

		}
		queries.insertGroupQuery.SetConsistency(gocql.Quorum)
		queries.insertGroupMemberQuery.SetConsistency(gocql.Quorum)
		queries.selectMembers.SetConsistency(gocql.LocalOne)
		db.queries[workerIndex] = queries
	}

	return nil
}

func (db *ScyllaDatabase) Close() {
	db.session.Close()
}