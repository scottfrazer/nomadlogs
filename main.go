package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	nomad "github.com/hashicorp/nomad/api"
	"github.com/olekukonko/tablewriter"
)

// always prefer the value specified at the command line
// if one isn't specified at the command line, use env var NOMAD_ADDR
// otherwise default to nomad's default address of http://127.0.0.1:4646
func getNomadAddr(cmdLineValue string) string {
	nomadAddr := cmdLineValue
	if len(nomadAddr) == 0 {
		nomadAddr = os.Getenv("NOMAD_ADDR")
	}
	if len(nomadAddr) == 0 {
		nomadAddr = nomad.DefaultConfig().Address
	}
	return nomadAddr
}

type nomadTask struct {
	job  string
	task string
}

type tailCommand struct {
	n          string
	follow     bool
	nomadTasks []nomadTask
	client     *nomad.Client
}

func (tail *tailCommand) Run() error {
	var wg sync.WaitGroup
	for _, task := range tail.nomadTasks {
		wg.Add(1)
		go func(task nomadTask) {
			defer wg.Done()
			watcher := NewWatcher(task.job, task.task, tail.client)
			lines := watcher.run()
			for line := range lines {
				fmt.Printf("%s\n", line.Format())
			}
		}(task)
	}
	wg.Wait()
	return nil
}

func NewTailCommand(n string, follow bool, addr string, tasks []string) (*tailCommand, error) {
	cfg := nomad.DefaultConfig()
	cfg.Address = addr
	client, err := nomad.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	var nomadTasks []nomadTask
	for _, task := range tasks {
		split := strings.Split(task, ":")
		if len(split) > 2 {
			return nil, fmt.Errorf("expecting 'job:task' or 'task', got %s", task)
		}
		if len(split) == 2 {
			nomadTasks = append(nomadTasks, nomadTask{split[0], split[1]})
		}
		if len(split) == 1 {
			nomadTasks = append(nomadTasks, nomadTask{"", split[0]})
		}
	}
	return &tailCommand{n, follow, nomadTasks, client}, nil
}

type allocation struct {
	allocationId string
	jobId        string
	task         string
	state        string
	taskGroup    string
	lastRestart  time.Time
}

func main() {
	// write meta logs to stderr, actual program output to stdout
	log.SetOutput(os.Stderr)
	log.SetPrefix("nomadlogs ")

	tailCmd := flag.NewFlagSet("tail", flag.ExitOnError)
	tailN := tailCmd.String("n", "10", "last n lines of logs use +NUM to start at line NUM")
	tailF := tailCmd.Bool("f", false, "follow logs")
	tailAddr := tailCmd.String("addr", "", "nomad address (e.g. http://127.0.0.1:4646)")

	lsCmd := flag.NewFlagSet("ls", flag.ExitOnError)
	lsAddr := lsCmd.String("addr", "", "nomad address (e.g. http://127.0.0.1:4646)")

	switch os.Args[1] {
	case "tail":
		tailCmd.Parse(os.Args[2:])
		cmd, err := NewTailCommand(*tailN, *tailF, getNomadAddr(*tailAddr), tailCmd.Args())
		if err != nil {
			log.Fatalf("NewTailCommand: %v\n", err)
		}
		if err := cmd.Run(); err != nil {
			log.Fatalf("Run: %v\n", err)
		}
	case "ls":
		cfg := nomad.DefaultConfig()
		cfg.Address = getNomadAddr(*lsAddr)
		client, err := nomad.NewClient(cfg)
		if err != nil {
			log.Fatalf("could not create nomad client: %v", err)
		}
		list, _, err := client.Allocations().List(nil)
		if err != nil {
			log.Fatalf("could not get allocations: %v", err)
		}

		type row struct {
			allocationId string
			jobId        string
			task         string
			state        string
			taskGroup    string
			lastRestart  time.Time
		}
		var rows []row
		for _, allocation := range list {
			for task, state := range allocation.TaskStates {
				rows = append(rows, row{allocation.ID, allocation.JobID, task, state.State, allocation.TaskGroup, state.LastRestart})
			}
		}

		sort.SliceStable(rows, func(i, j int) bool {
			strI := rows[i].jobId + rows[i].task
			strJ := rows[j].jobId + rows[j].task
			return strI < strJ
		})

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Allocation", "Job ID", "Task", "State", "Last Restart"})

		for _, row := range rows {
			lastRestart := ""
			if !row.lastRestart.IsZero() {
				lastRestart = row.lastRestart.Format("2006-01-02T15:04:05")
			}
			st := row.state
			if row.state == "running" {
				st = color.GreenString(row.state)
			}
			if row.state == "dead" {
				st = color.RedString(row.state)
			}
			table.Append([]string{row.allocationId[:8], row.jobId, row.task, st, lastRestart})
		}
		table.Render() // Send output
	case "download":
		fmt.Printf("not implemented yet\n")
	}
}

type logLine struct {
	job        string
	allocation *nomad.Allocation
	line       string
}

func (line logLine) Format() string {
	var parsed struct {
		Level   string    `json:"level"`
		Time    time.Time `json:"time"`
		Message string    `json:"message"`
		TraceId string    `json:"trace.id,omitempty"`
	}
	err := json.Unmarshal([]byte(line.line), &parsed)
	formatted := line.line
	if err == nil {
		formatted = fmt.Sprintf("[%s] [%s] %s", parsed.Time.Format("2006-01-02T15:04:05Z"), parsed.Level, parsed.Message)
	}

	return fmt.Sprintf("%s(%s): %s", color.CyanString(*line.allocation.Job.Name), color.GreenString(line.allocation.ID[:8]), formatted)
}

type watcher struct {
	job                string
	task               string
	client             *nomad.Client
	mu                 sync.Mutex
	allocationsWatched map[string]struct{}
	pollInterval       time.Duration
}

func NewWatcher(job, task string, client *nomad.Client) *watcher {
	return &watcher{job, task, client, sync.Mutex{}, make(map[string]struct{}), time.Second * 5}
}

func (jw *watcher) run() chan logLine {
	lines := make(chan logLine, 1000)
	go jw.poll(lines)
	return lines
}

func (jw *watcher) poll(lines chan logLine) {
	for range time.Tick(jw.pollInterval) {
		allocationList, _, err := jw.client.Allocations().List(nil)
		if err != nil {
			log.Printf("could not list nomad allocations. waiting %s before trying again: %s", jw.pollInterval, err)
			continue
		}

		for _, alloc := range allocationList {
			if _, ok := jw.allocationsWatched[alloc.ID]; ok {
				continue
			}
			if _, ok := alloc.TaskStates[jw.task]; !ok {
				continue
			}
			if jw.job != "" && jw.job != alloc.JobID {
				continue
			}
			if alloc.ClientStatus != "running" {
				continue
			}

			allocation, _, err := jw.client.Allocations().Info(alloc.ID, nil)
			if err != nil {
				log.Printf("could not retrieve allocation %s\n", alloc.ID)
				continue
			}

			go func(allocation *nomad.Allocation) {
				jw.mu.Lock()
				jw.allocationsWatched[allocation.ID] = struct{}{}
				jw.mu.Unlock()

				// watch the stream until it's done
				jw.watchAllocationLogs(allocation, lines)

				jw.mu.Lock()
				delete(jw.allocationsWatched, allocation.ID)
				jw.mu.Unlock()
			}(allocation)
		}
	}
}

func (jw *watcher) watchAllocationLogs(allocation *nomad.Allocation, lines chan logLine) error {
	stdoutFrames, stdoutErrChan := jw.client.AllocFS().Logs(allocation, true, jw.task, "stdout", "end", 0, nil, nil)
	stderrFrames, stderrErrChan := jw.client.AllocFS().Logs(allocation, true, jw.task, "stderr", "end", 0, nil, nil)

	for {
		select {
		case stdoutFrame, more := <-stdoutFrames:
			if !more {
				lines <- logLine{jw.job, allocation, "stdoutFrames closed!"}
				return nil
			}
			for _, line := range strings.Split(string(stdoutFrame.Data), "\n") {
				if line == "" {
					continue
				}
				lines <- logLine{jw.job, allocation, line}
			}
		case stderrFrame, more := <-stderrFrames:
			if !more {
				lines <- logLine{jw.job, allocation, "stderrFrames closed!"}
				return nil
			}
			for _, line := range strings.Split(string(stderrFrame.Data), "\n") {
				if line == "" {
					continue
				}
				lines <- logLine{jw.job, allocation, line}
			}
		case err := <-stdoutErrChan:
			if strings.Contains(err.Error(), "unknown task name") {
				return nil
			}
			log.Printf("%s: got error (allocation probably shutting down): %s", jw.job, err)
			return nil
		case err := <-stderrErrChan:
			if strings.Contains(err.Error(), "unknown task name") {
				return nil
			}
			log.Printf("%s: got error (allocation probably shutting down): %s", jw.job, err)
			return nil
		}
	}
}
