#!/usr/bin/env python2.6
import mapreduce


# CountWordsMapper/CountWordsReducer:
#   Counts the number of occurrences of each word in all documents.
class CountWordsMapper(mapreduce.Mapper):
    def Map(self, map_input):
        """
        map_input.key is the docid
        map_input.value is a string representing a document's contents
        Use mapreduce.Mapper.Output(self, key, value) to output (key, value)
        pairs from this mapper.
        """
        for word in map_input.value.split():
            mapreduce.Mapper.Output(self,word,1)

class CountWordsReducer(mapreduce.Reducer):
    def Reduce(self, reduce_input):
        """
        reduce_input.key is the input key
        Iterate over the values for this key using:
          for value in reduce_input:
        Write out output for this key using
          mapreduce.Reducer.Output(self, output_value)
        """
        total = 0
        for value in reduce_input:
            total  = total + value
            
        print reduce_input
            
              
if __name__ == "__main__":
    input_data = [(0, "the quick brown"),
                  (1, "fox jumps over"),
                  (2, "the lazy dog"),
                  (3, "the cow jumps over"),
                  (4, "the moon")]
    output_data = {}
    expected_output_data = {0: [('lazy', 1),
                                ('cow', 1),
                                ('jumps', 2),
                                ('fox', 1),
                                ('quick', 1),
                                ('the', 4),
                                ('over', 2)],
                            1: [('brown', 1),
                                ('dog', 1),
                                ('moon', 1)]}
    master = mapreduce.Master(CountWordsMapper, CountWordsReducer,
                              input_data, output_data,
                              num_workers=2,
                              num_output_shards=2,
                              split_size=2)
    master.Run()
    if output_data == expected_output_data:
        print "Successful mapreduce run."
    else:
        print "output_data does not match expected_output_data"
