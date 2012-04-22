
# Usage: Create a Master object, properly configured in its constructor,
# and invoke Run() on it.  Invoking Run(1) on the master will also print
# out some debug output that shows how the run is progressing.
#
# Input to the mapreduce is either a list of (key, value) tuples:
#    [(k1, v1), (k2, v2), ...]
# or a dictionary whose keys represent shard ids and values are a
# a list of (key, value) tuples:
#    {0 : [(k1, v1), (k2, v2)], 1 : [(k3, v3)], ...}
#
# Mapreduce output is a dictionary whose keys represent shard ids and values
# are a list of (key, value) tuples:
#    {0 : [(k1, v1), (k2, v2)], 1 : [(k3, v3)], ...}
#
# Note: This means that the output of a mapreduce can be used as the input
# to another mapreduce

class Master:
    def __init__(self, mapper_class, reducer_class, input_data, output_data,
                 num_workers, num_output_shards, split_size = 2):
        """Configures the Master with the basic information.
        output_data must be a dictionary, which is directly modified by
        this mapreduce.
        """
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        if isinstance(input_data, list):
            self.input_data = {0 : input_data}
        elif isinstance(input_data, dict):
            self.input_data = input_data
        else:
            print "Unknown type of input_data. Expected list or dict."
        self.output_data = output_data
        self.num_workers = num_workers
        self.workers = [Worker(self) for i in range(self.num_workers)]
        self.num_output_shards = num_output_shards
        self.split_size = split_size
        # shuffled_data holds data moved from mappers to reducers.
        # In a real Mapreduce implementation the shuffle happens on disk in a
        # distributed manner.
        self.output_data.clear()
        self.shuffled_data = {}
        for i in range(self.num_output_shards):
            self.output_data[i] = []
            self.shuffled_data[i] = []
        self.debug_level = 0
    def OutputShard(self, key):
        """Returns the output shard for a map output key."""
        return hash(key) % self.num_output_shards
    def Run(self, debug_level=0):
        self.debug_level = debug_level
        self.DebugPrint("Splitting input into chunks of size", self.split_size)
        # Split input data into chunks and assign to a worker to run a mapper.
        # A real mapreduce implementation would split into 16-64MB chunks
        self.next_worker_id = 0
        for shard_id in self.input_data:
            for i in range(0, len(self.input_data[shard_id]), self.split_size):
                self.input_data_split = self.input_data[shard_id][i:i + self.split_size]
                self.DebugPrint("Worker", self.next_worker_id,
                                ": Running", self.mapper_class.__name__,
                                "on shard:", shard_id,
                                "Input data start index:", i)
                self.workers[self.next_worker_id].RunMapper(self.input_data_split)
                self.DebugPrint("Worker", self.next_worker_id,
                                ": Map complete")
                self.DebugPrint("Worker", self.next_worker_id,
                                ": Shuffle map output to reduce shards.")
                self.workers[self.next_worker_id].Shuffle()
                self.next_worker_id = (self.next_worker_id + 1) % len(self.workers)
        # Now assign reducers to the workers
        for shard_id in range(self.num_output_shards):
            self.DebugPrint("Worker", self.next_worker_id,
                            ": Sorting keys for reduce shard", shard_id)
            self.workers[self.next_worker_id].PrepareReduceInput(shard_id)
            self.DebugPrint("Worker", self.next_worker_id,
                            ": Running", self.reducer_class.__name__,
                            "on reduce shard", shard_id)
            self.workers[self.next_worker_id].RunReducer(shard_id)
            self.DebugPrint("Worker", self.next_worker_id,
                            ": Reduce shard", shard_id, "complete")
            self.next_worker_id = (self.next_worker_id + 1) % len(self.workers)
        self.DebugPrint("Mapreduce complete.")
        self.debug_level = 0
    def DebugPrint(self, *args):
        if self.debug_level > 0:
            for arg in args:
                print arg,
            print ""

class Worker:
    def __init__(self, mapreduce_master):
        self.master = mapreduce_master
    def RunMapper(self, input_data_split):
        self.mapper = self.master.mapper_class()
        self.mapper.worker = self
        self.mapper.InitOutput()
        for key, value in input_data_split:
            self.map_input = MapInput(key, value)
            self.mapper.Map(self.map_input)
    def Shuffle(self):
        """Copy mapper output so reducers can work on it."""
        for shard_id in self.mapper.output:
            self.master.shuffled_data[shard_id].extend(self.mapper.output[shard_id])
    def RunReducer(self, shard_id):
        self.shard_id = shard_id
        self.reducer = self.master.reducer_class()
        self.reducer.worker = self
        for key in self.prepared_dict:
            self.reduce_input = ReduceInput(key, self.prepared_dict[key])
            self.reducer.Reduce(self.reduce_input)
    def PrepareReduceInput(self, shard_id):
        self.key_values = self.master.shuffled_data[shard_id]
        self.key_values.sort(None, lambda x: x[0])
        self.prepared_dict = {}
        for key, value in self.key_values:
            if isinstance(key, CompositeKey):
                key = key.key
            if key in self.prepared_dict:
                self.prepared_dict[key].append(value)
            else:
                self.prepared_dict[key] = [value]
        

class MapInput:
    def __init__(self, key, value):
        self.key = key
        self.value = value

class ReduceInput:
    def __init__(self, key, values):
        self.key = key
        self.values = values
    def __iter__(self):
        return iter(self.values)

class CompositeKey:
    def __init__(self, key, secondary_key):
        self.key = key
        self.secondary_key = secondary_key
    def __cmp__(self, other):
        if (self.key < other.key):
            return -1
        elif self.key == other.key:
            if self.secondary_key < other.secondary_key:
                return -1
            elif self.secondary_key == other.secondary_key:
                return 0
            else:
                return 1
        else:
            return 1

class Mapper:
    def __init__(self):
        # Worker should set this when it instantiates the mapper.  Worker
        # should also call InitOutput() at that time
        self.worker = None
    def InitOutput(self):
        # Map output from this mapper gets written to the self.output as follows:
        #   {0 : [(k1, v1), (k2, v2), (k1, v3)], 1 : [(k4, v4)], ...}
        #
        # In a real Mapreduce implementation, this dictionary would be written
        # to files on the local disk, partitioned into regions with one region
        # per output shard.
        self.output = {}
        for i in range(self.worker.master.num_output_shards):
            self.output[i] = []
    def Output(self, key, value):
        """Call this method to output (key, value) pairs from this mapper.
        """
        self.shard_id = self.worker.master.OutputShard(key)
        self.output[self.shard_id].append((key, value))
    def OutputWithSecondaryKey(self, key, secondary_key, value):
        """Call this method if you want the values for a particular key
        to be also sorted by secondary_key.
        """
        self.shard_id = self.worker.master.OutputShard(key)
        self.output[self.shard_id].append((CompositeKey(key, secondary_key),
                                           value))
    def Map(self, map_input):
        """Derived classes should override this method.
        map_input.key and map_input.value provide the input (key, value) pairs.
        """
        pass


class Reducer:
    def __init__(self):
        self.worker = None
    def Reduce(self, reduce_input):
        """Derived classes should override this method.
        You can iterate through the values simply as follows:
            for v in reduce_input:
                ...
        You can also get to the key using reduce_input.key
        """
        pass
    def Output(self, value):
        """Call this method to output the value for this reduce_input.  Note that
        you can't specify the key---it will be reduce_input.key
        """
        self.shard_data = self.worker.master.output_data[self.worker.shard_id]
        self.shard_data.append((self.worker.reduce_input.key, value))
