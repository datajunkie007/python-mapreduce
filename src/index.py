import mapreduce

# IndexMapper/IndexReducer
#   Create a reverse index from terms to docids for a collection of documents.
class IndexMapper(mapreduce.Mapper):
    def Map(self, map_input):
        """
        map_input.key is a docid
        map_input.value is a string representing the document's contents
        """
        pass
            
class IndexReducer(mapreduce.Reducer):
    def __init__(self):
        mapreduce.Reducer.__init__(self)
        self.postings_list = []
        self.previous_docid = -1
    def Reduce(self, reduce_input):
        """
        Output key should be a term in the documents.
        Output value should be the term's postings list.
        """
        pass

if __name__ == "__main__":
    input_data = [(0, "the quick brown"),
                  (2, "the lazy fox"),
                  (3, "the brown fox jumps over"),
                  (1, "fox jumps over"),
                  (4, "the moon")]
    output_data = {}
    expected_output_data = {0: [('lazy', [2]),
                                ('jumps', [1, 3]),
                                ('fox', [1, 2, 3]),
                                ('quick', [0]),
                                ('the', [0, 2, 3, 4]),
                                ('over', [1, 3])],
                            1: [('brown', [0, 3]),
                                ('moon', [4])]}
    master = mapreduce.Master(IndexMapper, IndexReducer,
                              input_data, output_data,
                              num_workers=2,
                              num_output_shards=2,
                              split_size=2)
    master.Run()
    if output_data == expected_output_data:
        print "Successful mapreduce run."
    else:
        print "output_data does not match expected_output_data"

