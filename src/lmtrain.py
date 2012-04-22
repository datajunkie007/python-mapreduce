import mapreduce

# LMMapper/LMReducer
#   Generates counts for the number of times each unigram and bigram occurs
#   in the input documents.  Also counts the the total number of unigrams
#   and bigrams in the documents.
class LMMapper(mapreduce.Mapper):
    def Map(self, map_input):
        """
        map_input.key is a docid
        map_input.value is a string representing the document's contents
        """
        pass

class LMReducer(mapreduce.Reducer):
    def Reduce(self, reduce_input):
        """
        Output key should be a unigram (a string) or a bigram (a tuple of
        strings).  Strings __num_unigrams__ and __num_bigrams__ should
        represent the number of unigrams and bigrams, respectively
        The value should be the count of that unigram/bigram.
        """
        pass

if __name__ == "__main__":
    input_data = [(0, "the quick brown"),
                  (1, "fox jumps over"),
                  (2, "the lazy dog"),
                  (3, "the cow jumps over"),
                  (4, "the moon")]
    output_data = {}
    expected_output_data = {0: [(('the', 'moon'), 1),
                                ('lazy', 1),
                                ('cow', 1),
                                ('jumps', 2),
                                ('fox', 1),
                                (('quick', 'brown'), 1),
                                (('lazy', 'dog'), 1),
                                ('quick', 1),
                                ('the', 4),
                                ('over', 2)],
                            1: [('brown', 1),
                                (('jumps', 'over'), 2),
                                (('the', 'lazy'), 1),
                                (('cow', 'jumps'), 1),
                                ('dog', 1),
                                (('the', 'quick'), 1),
                                ('moon', 1),
                                ('__num_bigrams__', 10),
                                ('__num_unigrams__', 15),
                                (('fox', 'jumps'), 1),
                                (('the', 'cow'), 1)]}
    master = mapreduce.Master(LMMapper, LMReducer,
                              input_data, output_data,
                              num_workers=2,
                              num_output_shards=2,
                              split_size=2)
    master.Run()
    if output_data == expected_output_data:
        print "Successful mapreduce run."
    else:
        print "output_data does not match expected_output_data"
