package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gookit/color"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"
)

type DatabaseType uint
const (
	UnknownDatabase = DatabaseType(0)
	ScyllaDb = DatabaseType(1)
	RethinkDb = DatabaseType(2)
)

func (databaseType DatabaseType) String() string {
	switch databaseType {
	case ScyllaDb:
		return "scylladb"
	case RethinkDb:
		return "rethinkdb"
	default:
		return "unknown"
	}
}

func (databaseType *DatabaseType) Set(value string) error {
	switch strings.ToLower(value) {
	case "scylladb":
		*databaseType = ScyllaDb
		break
	case "rethinkdb":
		*databaseType = RethinkDb
		break
	default:
		*databaseType = UnknownDatabase
		break
	}
	return nil
}

type BenchmarkType uint
const (
	UnknownBenchmark = BenchmarkType(0)
	FillData = BenchmarkType(1)
	SelectMembers = BenchmarkType(2)
)

func (benchmarkType BenchmarkType) String() string {
	switch benchmarkType {
	case FillData:
		return "filldata"
	case SelectMembers:
		return "selectmembers"
	default:
		return "unknown"
	}
}

func (benchmarkType *BenchmarkType) Set(value string) error {
	switch strings.ToLower(value) {
	case "filldata":
		*benchmarkType = FillData
		break
	case "selectmembers":
		*benchmarkType = SelectMembers
		break
	default:
		*benchmarkType = UnknownBenchmark
		break
	}
	return nil
}

type StringListFlags []string

func (i *StringListFlags) String() string {
	return strings.Join(*i, ",")
}

func (i *StringListFlags) Set(value string) error {
	*i = strings.Split(value, ",")
	for j, v := range *i {
		(*i)[j] = strings.TrimSpace(v)
	}
	return nil
}

type ProgramArguments struct {
	InstanceId         int
	InstanceCount      int
	ParallelQueryCount int
	CpuCores		   int

	DatabaseType              DatabaseType
	DatabaseEndpoints         StringListFlags
	DatabaseName              string
	DatabaseCreate            bool
	DatabaseReplicationFactor int
	DatabaseTimeout           time.Duration

	FirstLevelGroupCount int
	LevelGroupFactor     float64
	LevelCount           int
	UserMemberCount      int
	SubgroupMemberCount  int


	BenchmarkType				BenchmarkType

	StatisticsSnapshotPeriod   time.Duration

//	Background bool
//	Verbose bool
}

type flagUsageWriter struct {}

func (e flagUsageWriter) Write(p []byte) (int, error) {
	color.Error.Print(string(p))
	return len(p), nil
}

func (arguments *ProgramArguments) PrintDefaults(applicationName string) {
	color.Info.Println(fmt.Sprintf("Usage of %s:", applicationName))
	flag.VisitAll(func(f *flag.Flag) {
		s := fmt.Sprintf("  -%s", f.Name) // Two spaces before -; see next two comments.
		name, usage := flag.UnquoteUsage(f)
		if len(name) > 0 {
			s += " " + name
		}
		// Boolean flags of one ASCII letter are so common we
		// treat them specially, putting their usage on the same line.
		if len(s) <= 4 { // space, space, '-', 'x'.
			s += "\t"
		} else {
			// Four spaces before the tab triggers good alignment
			// for both 4- and 8-space tab stops.
			s += "\n    \t"
		}
		s += strings.ReplaceAll(usage, "\n", "\n    \t")


		if !func(f *flag.Flag, value string) bool {
			// Build a zero value of the flag's Value type, and see if the
			// result of calling its String method equals the value passed in.
			// This works unless the Value type is itself an interface type.
			typ := reflect.TypeOf(f.Value)
			var z reflect.Value
			if typ.Kind() == reflect.Ptr {
				z = reflect.New(typ.Elem())
			} else {
				z = reflect.Zero(typ)
			}
			return value == z.Interface().(flag.Value).String()
		}(f, f.DefValue) {
			if name == "string" {
				// put quotes on the value
				s += fmt.Sprintf(" (default %q)", f.DefValue)
			} else {
				s += fmt.Sprintf(" (default %v)", f.DefValue)
			}
		}
		color.Info.Println(s)
	})
}

func (arguments *ProgramArguments) Print() {
	s, _ := json.MarshalIndent(arguments, "", "\t")
	color.Info.Println("Current config:")
	color.Info.Println(string(s))
}

func (arguments *ProgramArguments) Parse() bool {
	var wrongArguments bool = false
	var applicationName string = filepath.Base(os.Args[0])

	flag.CommandLine.Usage = func() {
		arguments.PrintDefaults(applicationName)
	}
	flag.CommandLine.SetOutput(&flagUsageWriter{})
	flag.IntVar(&arguments.InstanceId,"instance-number", 1, "index of instance starting at 1")
	flag.IntVar(&arguments.InstanceCount,"instance-count", 1, "number of instances")
	flag.IntVar(&arguments.ParallelQueryCount,"parallel-query-count", 32, "number of parallel queries")
	flag.IntVar(&arguments.CpuCores,"cpu-cores", 1, "limit of CPU cores")

	flag.Var(&arguments.DatabaseType, "database-type", "type of database: ScyllaDb or RethinkDb")
	flag.Var(&arguments.DatabaseEndpoints, "database-endpoints","list of database addresses. For example: \"192.168.0.56, 192.168.0.65, 192.168.0.64:24233\"")
	flag.StringVar(&arguments.DatabaseName, "database-name", "dbperf", "name of database")
	flag.BoolVar(&arguments.DatabaseCreate, "database-recreate-tables", true, "drop and create tables in the database")
	flag.IntVar(&arguments.DatabaseReplicationFactor,"database-replication-factor", 1, "replication factor of database")
	flag.DurationVar(&arguments.DatabaseTimeout,"database-timeout",10000, "timeout in milliseconds")

	flag.IntVar(&arguments.FirstLevelGroupCount,"first-level-group-count", 1000, "number of groups on the first level")

	flag.Float64Var(&arguments.LevelGroupFactor,"level-group-factor", 1.0, "ratio of the number of groups for each next level")
	flag.IntVar(&arguments.LevelCount,"level-count", 5, "number of levels")

	flag.IntVar(&arguments.UserMemberCount,"user-member-count", 90, "number of user members in the group")
	flag.IntVar(&arguments.SubgroupMemberCount,"subgroup-member-count", 10, "number of subgroups in the group")

	flag.DurationVar(&arguments.StatisticsSnapshotPeriod, "statistics-snapshot-period", 1000, "statistics interval in milliseconds")
	flag.Var(&arguments.BenchmarkType, "benchmark-type", "type of benchmark: FillData or SelectMembers")

//	flag.BoolVar(&arguments.Verbose,"verbose", false, "verbose output")
//	flag.BoolVar(&arguments.Background,"background", false, "run in background")
	flag.Parse()

	checkArguments := func(argumentName string, required bool, condition bool, wrongArguments* bool) bool {
		var found bool = false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == argumentName {
				found = true
			}
		})
		if required == true && found == false {
			color.Error.Println(fmt.Sprintf("%s: missing argument --%s", applicationName, argumentName))
			*wrongArguments = true
			return false
		} else
		if condition == false {
			color.Error.Println(fmt.Sprintf("%s: invalid argument value --%s", applicationName, argumentName))
			*wrongArguments = true
			return false
		}
		return true
	}

	checkArguments("database-type", true, arguments.DatabaseType != UnknownDatabase, &wrongArguments)
	checkArguments("database-endpoints", true, len(arguments.DatabaseEndpoints) > 0, &wrongArguments)
	checkArguments("database-name", false, arguments.DatabaseName != "", &wrongArguments)
	checkArguments("cpu-cores", false, arguments.CpuCores > 0 && arguments.CpuCores <= runtime.NumCPU(), &wrongArguments)
	checkArguments("benchmark-type", true, arguments.BenchmarkType != UnknownBenchmark, &wrongArguments)
	checkArguments("instance-count", false, arguments.InstanceCount > 0, &wrongArguments)
	checkArguments("instance-number", false, arguments.InstanceId > 0 && arguments.InstanceId <= arguments.InstanceCount, &wrongArguments)

	if Config.BenchmarkType != FillData {
		Config.DatabaseCreate = false
		Config.InstanceId = 1
		Config.InstanceCount = 1
	}

	if wrongArguments {
		arguments.PrintDefaults(applicationName)
		return false
	}
	return true
}
