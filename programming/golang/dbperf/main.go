package main

import (
	"fmt"
	"github.com/gookit/color"
	"runtime"
)

var Config ProgramArguments

func main() {

	if !Config.Parse() {
		return
	}
	runtime.GOMAXPROCS(Config.CpuCores)
	Config.Print()

	var database Database
	switch Config.DatabaseType {
	case ScyllaDb:
		{
			database = &ScyllaDatabase{}
			break
		}
	case RethinkDb:
		{
			database = &RethinkDatabase{}
			break
		}
	default:
		{
			color.Error.Println("Database is not implemented")
			break
		}
	}

	var err error
	if err = database.Initialize(); err != nil {
		color.Error.Println(fmt.Sprintf("Unable to initialize database: %s", err))
		return
	}

	switch Config.BenchmarkType {
	case FillData:
		{
			if err = database.FillData(); err != nil {
				color.Error.Println(fmt.Sprintf("Unable to insert data into database: %s", err))
			}
			break
		}
	case SelectMembers:
		{
			if err = database.SelectMembers(); err != nil {
				color.Error.Println(fmt.Sprintf("Unable to select members from database: %s", err))
			}
			break

		}
	default:
		{
			color.Error.Println("Benchmark is not implemented")
			break
		}
	}

	database.Close()

}
