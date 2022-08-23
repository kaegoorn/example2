package main

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/gookit/color"
	"math"
	"sync"
	"time"
)

type Database interface {
	Initialize() error
	FillData() error
	SelectMembers() error
	Close()

	insertGroup(workerIndex int, groupLevel int, groupNumber int) error
	insertGroupMember(workerIndex int, groupLevel int, groupNumber int, memberIndex int, subgroup bool) error

	selectMembers(workerIndex int, job *SelectMembersJob) error
}

type DatabaseHelper struct {
}

func customUUID(level uint16, number uint32) (uuid.UUID) {
	var u uuid.UUID

	binary.BigEndian.PutUint32(u[0:], number)
	binary.BigEndian.PutUint16(u[4:], level)
	u[6] &= 0x0F // clear version
	u[6] |= 0x40 // set version to 4 (random uuid)
	u[8] &= 0x3F // clear variant
	u[8] |= 0x80 // set to IETF variant
	return u
}

func (helper *DatabaseHelper) FillData(db Database) error {
	color.Notice.Println("fill data...")
	var mu sync.Mutex
	var wg sync.WaitGroup

	var currentLevel int =  0
	var groupLastId int = 0
	var currentGroup int = -1
	var totalGroupCountInLevel int
	var totalGroupProcessed int = 0
	var totalGroups int
	var currentGroupMemberIndex int = 0
	var totalMemberCountInGroup = Config.UserMemberCount + Config.SubgroupMemberCount
	for i := 0; i < Config.LevelCount; i++ {
		totalGroups += int(float64(Config.FirstLevelGroupCount) * math.Pow(Config.LevelGroupFactor, float64(i)))
	}

	var queryCount int64 = 0
	var statisticsStart time.Time = time.Now()
	var minLatency time.Duration = 1 * time.Hour
	var maxLatency time.Duration = 0
	var totalLatency time.Duration = 0

	printStats := func(elapsed time.Duration) {
		color.Notice.Println(fmt.Sprintf("[fill-data %3d%%] level: %d/%d, group: %10d/%d, member: %10d/%d, queries: %8d, queries per second: %d, latency min/max/avg (ms): %d/%d/%d", int(totalGroupProcessed * 100 / totalGroups), currentLevel, Config.LevelCount, currentGroup, groupLastId, currentGroupMemberIndex, totalMemberCountInGroup, queryCount, int(float64(queryCount) / elapsed.Seconds()), int(minLatency.Milliseconds()), int(maxLatency.Milliseconds()), int(totalLatency.Milliseconds() / queryCount)))
		statisticsStart = time.Now()
		queryCount = 0
		minLatency = 1 * time.Hour
		maxLatency = 0
		totalLatency = 0
	}
	for workerIndex := 0; workerIndex < Config.ParallelQueryCount; workerIndex++ {
		wg.Add(1)
		go func(workerIndex int) {
			var currentLatency time.Duration

			for {
				mu.Lock()

				if currentLatency < minLatency {
					minLatency = currentLatency
				}
				if currentLatency > maxLatency {
					maxLatency = currentLatency
				}
				totalLatency += currentLatency

				if currentGroupMemberIndex == 0 {
					currentGroup++
					totalGroupProcessed++
				}
				if currentGroup == groupLastId {
					if currentLevel > 0 && currentLevel <= Config.LevelCount {
						printStats(time.Now().Sub(statisticsStart))
					}
					currentLevel++
					totalGroupCountInLevel = int(float64(Config.FirstLevelGroupCount) * math.Pow(Config.LevelGroupFactor, float64(currentLevel-1)))
					currentGroup = (Config.InstanceId - 1) * totalGroupCountInLevel / Config.InstanceCount
					groupLastId = currentGroup + totalGroupCountInLevel / Config.InstanceCount
				}
				if currentLevel > Config.LevelCount {
					mu.Unlock()
					break
				}


				//if workerCurrentGroupMemberIndex == Config.FirstLevelGroupCount
				var workerCurrentLevel int = currentLevel
				var workerCurrentGroup int = currentGroup

				currentGroupMemberIndex++
				totalMemberCountInGroup = int(float64(Config.UserMemberCount) * math.Pow(Config.LevelGroupFactor, float64(currentLevel-1))) + int(float64(Config.SubgroupMemberCount) * math.Pow(Config.LevelGroupFactor, float64(currentLevel-1)))
				if currentGroupMemberIndex > totalMemberCountInGroup {
					currentGroupMemberIndex = 0
				}
				var workerCurrentGroupMemberIndex int = currentGroupMemberIndex

				queryCount++

				// print stats
				var elapsed time.Duration = time.Now().Sub(statisticsStart)
				if elapsed >= Config.StatisticsSnapshotPeriod*time.Millisecond {
					printStats(elapsed)
				}
				mu.Unlock()

				var operationStart time.Time = time.Now()
				if workerCurrentGroupMemberIndex == 0 || workerCurrentLevel == 1 {
					if err := db.insertGroup(workerIndex, workerCurrentLevel, workerCurrentGroup); err != nil {
						color.Error.Println(err)
					}
				} else  {
					if err := db.insertGroupMember(workerIndex, workerCurrentLevel, workerCurrentGroup,  workerCurrentGroupMemberIndex, workerCurrentLevel > 2 && workerCurrentGroupMemberIndex > int(float64(Config.UserMemberCount) * math.Pow(Config.LevelGroupFactor, float64(workerCurrentLevel-1)))); err != nil {
						color.Error.Println(err)
					}
				}
				currentLatency = time.Now().Sub(operationStart)
			}
			wg.Done()
		}(workerIndex)
	}
	wg.Wait()

	return nil
}

type SelectMembersJob struct {
	groups []uuid.UUID
	members map[uuid.UUID]string
	duration time.Duration
	subgroupCount int
	deep int
}

func (helper *DatabaseHelper) SelectMembers(db Database) error {
	var topLevelGroups [] uuid.UUID
	getLastLevelGroups(&topLevelGroups)
	var currentTopLevelGroup int = -1
	color.Notice.Println(fmt.Sprintf("top level groups: %d", len(topLevelGroups)))

	var jobs chan *SelectMembersJob
	jobs = make(chan *SelectMembersJob)

	var mu sync.Mutex
	var wg sync.WaitGroup

	var queryCount int64 = 0
	var statisticsStart time.Time = time.Now()
	var totalGroupProcessed int = 0

	var minLatency time.Duration = 1 * time.Hour
	var maxLatency time.Duration = 0
	var totalLatency time.Duration = 0

	var totalMembers int  = 0
	var totalSubgroups int  = 0
	var totalDeep int  = 0

	printStats := func(elapsed time.Duration) {
		color.Notice.Println(fmt.Sprintf("[select-members %3d%%] group: %10d/%d, queries: %8d, queries per second: %d, latency min/max/avg (ms): %d/%d/%d, avg subrequests: %d, avg subgroups: %d, avg members: %d ", int(totalGroupProcessed * 100 / len(topLevelGroups)), totalGroupProcessed, len(topLevelGroups), queryCount, int(float64(queryCount) / elapsed.Seconds()), int(minLatency.Milliseconds()), int(maxLatency.Milliseconds()), int(totalLatency.Milliseconds() / queryCount), int(int64(totalDeep) / queryCount), int(int64(totalSubgroups) / queryCount), int(int64(totalMembers) / queryCount)))
		statisticsStart = time.Now()
		queryCount = 0
		minLatency = 1 * time.Hour
		maxLatency = 0
		totalLatency = 0
		totalMembers = 0
		totalSubgroups = 0
		totalDeep = 0
	}

	for workerIndex := 0; workerIndex < Config.ParallelQueryCount; workerIndex++ {
		wg.Add(1)
		go func(workerIndex int) {

			for {
				select {
				case job := <-jobs:
					if err := db.selectMembers(workerIndex, job); err != nil {
						color.Warn.Println(fmt.Sprintf("Unable to select members: %s", err))
					}
					mu.Lock()

					totalMembers += len(job.members)
					totalSubgroups += job.subgroupCount
					totalDeep += job.deep

					if job.duration < minLatency {
						minLatency = job.duration
					}
					if job.duration > maxLatency {
						maxLatency = job.duration
					}
					totalLatency += job.duration

					queryCount++
					totalGroupProcessed++

					var elapsed time.Duration = time.Now().Sub(statisticsStart)
					if elapsed >= Config.StatisticsSnapshotPeriod*time.Millisecond {
						printStats(elapsed)
					}
					mu.Unlock()
					if len(job.groups) == 0 {
						continue
					}
					continue
				default:
					mu.Lock()
					currentTopLevelGroup++
					mu.Unlock()
					if currentTopLevelGroup < len(topLevelGroups) {
						var job *SelectMembersJob = &SelectMembersJob {
							members: make(map[uuid.UUID]string),
						}
						job.groups = append(job.groups, uuid.UUID(topLevelGroups[currentTopLevelGroup]))
						jobs <- job
						continue
					}
					break
				}
				break
			}
			wg.Done()
		}(workerIndex)
	}
	wg.Wait()

	printStats(time.Now().Sub(statisticsStart))

	return nil
}
