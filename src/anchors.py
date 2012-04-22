import mapreduce

# AnchorMapper/AnchorReducer
#   Attaches anchor text to the target of the links.
class AnchorMapper(mapreduce.Mapper):
    def Map(self, map_input):
        """
        map_input.key is a docid
        map_input.value is a list with two types of items:
           If its a string, it represents the body text of the document
           If its a tuple, it has the form (target_doc_id, anchor_text)
             where target_docid is the document pointed to by this link
             and anchor_text is the text f that link.
        """
        pass

class AnchorReducer(mapreduce.Reducer):
    def Reduce(self, reduce_input):
        """
        Output key should be a docid.
        Output value should be a string representing words that should be
          associated with the docid.  These words could be body words or
          anchor text.  You should output the body text and each of the anchor
          text features separately (all with the same key)
        """
        pass

if __name__ == "__main__":
    input_data = [(0, ["the quick brown", (1, "jumping fox"), (4, "lunar")]),
                  (1, ["fox jumps over", (0, "fast and brown")]),
                  (2, ["the lazy fox", (3, "cow"), (4, "moon")]),
                  (3, ["the brown fox jumps over", (1, "jumps high")]),
                  (4, ["the moon"])]
    output_data = {}
    expected_output_data = {0: [(0, 'the quick brown'),
                                (0, 'fast and brown'),
                                (2, 'the lazy fox'),
                                (4, 'lunar'),
                                (4, 'moon'),
                                (4, 'the moon')],
                            1: [(1, 'jumping fox'),
                                (1, 'fox jumps over'),
                                (1, 'jumps high'),
                                (3, 'cow'),
                                (3, 'the brown fox jumps over')]}
    master = mapreduce.Master(AnchorMapper, AnchorReducer,
                              input_data, output_data,
                              num_workers=2,
                              num_output_shards=2,
                              split_size=2)
    master.Run()
    if output_data == expected_output_data:
        print "Successful mapreduce run."
    else:
        print "output_data does not match expected_output_data"
