package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

type Empty struct{}

const (
	IDLE = iota
	INPROGRESS
	FAILED
)

type WorkerInfo struct {
	ID    uuid.UUID
	state int
	task  Task
	mu    sync.RWMutex
}

type Task struct {
	Name      string
	InputFile string
	Num       int
}

type TaskReport struct {
	ID       uuid.UUID
	FileList []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
