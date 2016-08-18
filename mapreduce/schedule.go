package mapreduce

import "fmt"
import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

    //hand out the map and reduce tasks to workers, and return only when all the tasks have finished.
    //schedule only needs to tell the workers the name of the original input file (mr.files[task]) and the task

    var wg sync.WaitGroup
    for i := 0; i < ntasks; i++ {

    	wg.Add(1)
    	go func(wg *sync.WaitGroup, taskNum int) {

            defer wg.Done()
            for {
				//select worker
				worker :=  <- mr.registerChannel

				//construct DoTaskArgs
				args := new(DoTaskArgs)
				args.JobName = mr.jobName
				args.TaskNumber = taskNum
				args.File = mr.files[taskNum]
				args.NumOtherPhase = nios
				args.Phase = phase

                //inform worker to dotask
				ok := call(worker, "Worker.DoTask", args, new(struct{}))
				if ok == false {
				    fmt.Printf("fail to connect\n")
				} else {
					//collect worker resource
                    go func() {
		                mr.registerChannel <- worker
	                }()
                    break;
				}
            }
    	}(&wg, i)
    }

    wg.Wait()
    fmt.Println("Group done")
	fmt.Printf("Schedule: %v phase done\n", phase)
}
