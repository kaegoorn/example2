package main

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/gookit/color"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"math"
	"math/rand"
	"time"
)

type RethinkQueries struct {
	insertGroupQuery *gocql.Query
	insertGroupMemberQuery *gocql.Query
}

type RethinkDatabase struct {
	session *r.Session
	context context.Context
}

func (db *RethinkDatabase) createDatabase() error {
	color.Notice.Println("creating database...")
	var err error
	err = r.DB(Config.DatabaseName).TableDrop("groups").Exec(db.session)
	err = r.DB(Config.DatabaseName).TableCreate("groups", r.TableCreateOpts {
		PrimaryKey: "id",
		Shards: Config.DatabaseReplicationFactor,
		Replicas: Config.DatabaseReplicationFactor,

	}).Exec(db.session)
	if err != nil {
		return err
	}
/*
	_, err = r.DB(Config.DatabaseName).Table("groups").IndexCreate("id").RunWrite(db.session)
	if err != nil {
		return err
	}
*/

	err = r.DB(Config.DatabaseName).TableDrop("group_members").Exec(db.session)
	err = r.DB(Config.DatabaseName).TableCreate("group_members", r.TableCreateOpts {
		PrimaryKey: "id",
		Shards: Config.DatabaseReplicationFactor,
		Replicas: Config.DatabaseReplicationFactor,

	}).Exec(db.session)
	if err != nil {
		return err
	}

	_, err = r.DB(Config.DatabaseName).Table("group_members").IndexCreate("group_id").RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkDatabase) insertGroup(workerIndex int, groupLevel int, groupNumber int) error {
	var groupId uuid.UUID
	var err error
	var groupType int

	groupId = customUUID(uint16(groupLevel), uint32(groupNumber + 1))

	if groupLevel == 1 {
		groupType = 1
	} else {
		groupType = 2
	}

	err = r.DB(Config.DatabaseName).Table("groups").Insert(map[string]interface{}{
		"id":  groupId.String(),
		"type": groupType,
	}, r.InsertOpts{
		Conflict: "replace",
	}).Exec(db.session)
	if err != nil {
		return err
	}
	return nil
}

func (db *RethinkDatabase) insertGroupMember(workerIndex int, groupLevel int, groupNumber int, memberIndex int, group bool) error {
	var groupId uuid.UUID
	var memberId uuid.UUID
	var err error
	var memberGroupLevel int = 1
	var memberNumber int = 1
	var totalGroupCountInLevel int
	groupId = customUUID(uint16(groupLevel), uint32(groupNumber + 1))

	if group {
		memberGroupLevel = groupLevel - 1
		totalGroupCountInLevel = int(float64(Config.FirstLevelGroupCount) * math.Pow(Config.LevelGroupFactor, float64(groupLevel - 2)))
	} else {
		totalGroupCountInLevel = Config.FirstLevelGroupCount
	}
	memberNumber = rand.Intn(totalGroupCountInLevel)


	memberId = customUUID(uint16(memberGroupLevel), uint32(memberNumber + 1))

	err = r.DB(Config.DatabaseName).Table("group_members").Insert(map[string]interface{}{
		"id":  groupId.String() + ":" + memberId.String(),
		"group_id":  groupId.String(),
		"member_id": memberId.String(),
	}, r.InsertOpts{
		Conflict: "replace",
	}).Exec(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkDatabase) selectMembers(workerIndex int, job *SelectMembersJob) error {
	var start time.Time = time.Now()
	var err error
	for len(job.groups) > 0 {
		var n int = len(job.groups)

		var params []interface{} = make([]interface{}, n)
		for i := 0; i < n; i++ {
			params[i] = job.groups[i].String()
		}
		job.groups = job.groups[n:]

		job.deep++

		var cursor *r.Cursor
		if cursor, err = r.DB(Config.DatabaseName).Table("group_members").GetAllByIndex("group_id", params...).EqJoin("member_id", r.DB(Config.DatabaseName).Table("group_members"), r.EqJoinOpts {
			Index: "group_id",
		}).Run(db.session); err != nil {
			return err
		}

		type memberRecord map[string]string
		var member memberRecord
		for cursor.Next(&member) {
			if err = cursor.Err(); err != nil {
				cursor.Close()
				return err
			}
//			var u uuid.UUID
//			u, _ = uuid.Parse(member["member_id"])
//			job.groups = append(job.groups, u)
			job.subgroupCount++
		}
		if cursor.Err() != nil {
			return err
		}


	}

	job.duration = time.Now().Sub(start)
	return nil
}

func (db *RethinkDatabase) FillData() error {
	helper := DatabaseHelper{}
	if err := helper.FillData(db); err != nil {
		return err
	}
	return nil
}

func (db *RethinkDatabase) SelectMembers() error {
	var helper DatabaseHelper
	helper.SelectMembers(db)
	return nil
}

func (db *RethinkDatabase) Initialize() error {
	var err error

	if db.session, err = r.Connect(r.ConnectOpts{
		Addresses: Config.DatabaseEndpoints,
		Database: Config.DatabaseName,
		Timeout: Config.DatabaseTimeout * time.Millisecond,
		InitialCap: 8,
		MaxOpen:    8,
	}); err != nil {
		return err
	}

	db.context = context.Background()
	if Config.DatabaseCreate == true {
		if err = db.createDatabase(); err != nil {
			return err
		}
	}

	return nil
}

func (db *RethinkDatabase) Close() {
	db.session.Close()
}