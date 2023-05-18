package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// declare an argument structure.
		args := TaskRequest{}

		// declare a reply structure.
		reply := TaskResponse{}
		// send the RPC request, wait for the reply.
		CallGetTask(&args, &reply)
		filename := reply.XTask.FileName
		id := strconv.Itoa(reply.XTask.Id)

		if filename != "" {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open maptask :%v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read :%v", filename)
			}
			file.Close()
			// mapf
			kva := mapf(filename, string(content))
			num_reduce = reply.NumReduceTasks
			bucket := make([][]KeyValue, num_reduce)

			for _, kv := range kva {
				// hash the key and put it into the corresponding bucket
				bucket[ihash(kv.Key) % num_reduce] = append(bucket[ihash(kv.Key) % num_reduce], kv)
			}

			for i := 0; i < num_reduce; i++ {
				tmp_file, err := ioutil.TempFile(".", "mr-map-*")
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				// json encode
				enc := json.NewEncoder(tmp_file)
				err := enc.Encode(&bucket[i])
				if err != nil {
					log.Fatalf("cannot encode bucket")
				}
				tmp_file.Close()
				// rename
				out_file := "mr-" + strconv.Itoa(i) + "-" + id
				err := os.Rename(tmp_file.Name(), out_file)
				if err != nil {
					log.Fatalf("cannot rename temp file")
				}
			}
			// map done
			reply.MapDone <- true
		} else {
			num := reply.NumMapTasks
			id := strconv.Itoa(reply.XTask.IdReduce)
			if len(reply.MapDone) == num {
				// start reduce
				kva := []KeyValue{}
				for i := 0; i < num; i++ {
					map_filename := "mr-" + strconv.Itoa(i) + "-" + id
					inputFiles, err := os.OpenFile(map_filename, os.O_RDONLY, 0777)
					if err != nil {
						log.Fatalf("cannot open reduce task file :%v", map_filename)
					}
					dec := json.NewDecoder(inputFiles)
					for {
						var kv []KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv...)
					}
				}
				out_file := "mr-out-" + id
				tmp_file, err := ioutil.TempFile(".", "mr-out-*")
				if err != nil {
					log.Fatalf("cannot create temp file")
				}

				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)
			
					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(tmp_file, "%v %v\n", kva[i].Key, output)
			
					i = j
				}

				tmp_file.Close()
				os.Rename(tmp_file.Name(), out_file)
			}
		} 
		if len(reply.MapDone) == reply.NumMapTasks && reply.XTask.FileName == "" {
			break
		}
	}
	
	
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask(args *TaskRequest, reply *TaskResponse) {

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Name %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
