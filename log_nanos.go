package lognanos

import (
	"fmt"
	"github.com/bashar-saleh/gonanos/nanos"
	"log"
	"os"
	"strconv"
	"time"
)



func NewLogNanos(
	workersMaxCount int,   // equal to number of log files
	taskQueueCapacity int,
	baseFilename string,
) chan nanos.Message {

	worker := newLogsWorker(workersMaxCount, baseFilename)

	myNanos := nanos.Nanos{
		WorkersMaxCount:   workersMaxCount,
		TaskQueueCapacity: taskQueueCapacity,
		Worker:            &worker,
	}

	return myNanos.TasksChannel()

}

func newLogsWorker(numberOfFiles int, baseFilename string) logsWorker {
	keyCabinets := make(chan int, numberOfFiles)

	for i := 0; i < numberOfFiles; i++ {
		// we can user any other id
		keyCabinets <- i
	}

	return logsWorker{
		baseFilename: baseFilename,
		keyCabinets:  keyCabinets,
	}

}

type logsWorker struct {
	baseFilename string
	keyCabinets  chan int
}

func (w *logsWorker) Work(msg nanos.Message) {

	// pull a key
	key := <-w.keyCabinets

	// extract the msg content
	msgContent := string(msg.Content)

	f, err := os.OpenFile(w.baseFilename+"_"+strconv.Itoa(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		select {
		case msg.ErrTo <- err:
			return
		default:
			return
		}
	}
	defer func() {
		err := f.Close()
		// return the key to key cabinets
		w.keyCabinets <- key

		if err != nil {
			log.Printf("File %s can't be close", w.baseFilename+"_"+strconv.Itoa(key))
		}
	}()

	_, err = f.WriteString(fmt.Sprintf("%v -- %v \n", time.Now().String(), msgContent))
	if err != nil {
		log.Printf("File %s can't be write to", w.baseFilename+"_"+strconv.Itoa(key))
	}

}
