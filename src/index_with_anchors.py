import mapreduce
import anchors
import index

if __name__ == "__main__":
    input_data = [(0, ["the quick brown", (1, "jumping fox"), (4, "lunar")]),
                  (2, ["the lazy fox", (3, "cow"), (4, "moon")]),
                  (1, ["fox jumps over", (0, "fast and brown")]),
                  (3, ["the brown fox jumps over", (1, "jumps high")]),
                  (4, ["the moon"])]
    index_expected_output_data = {0: [('and', [0]),
                                      ('lazy', [2]),
                                      ('cow', [3]),
                                      ('jumps', [1, 3]),
                                      ('fox', [1, 2, 3]),
                                      ('fast', [0]),
                                      ('high', [1]),
                                      ('quick', [0]),
                                      ('the', [0, 2, 3, 4]),
                                      ('over', [1, 3])],
                                  1: [('brown', [0, 3]),
                                      ('jumping', [1]),
                                      ('lunar', [4]),
                                      ('moon', [4])]}
    index_output_data = {}

    # Fill in appropriate mapreduce calls that use AnchorMapper/AnchorReducer
    # and IndexMapper/IndexReducer to use input_data as input and produce
    # index_expected_output_data as output.
    # Write out your output to index_output_data

    pass

    if index_output_data == index_expected_output_data:
        print "Successful mapreduce run."
    else:
        print "output_data does not match expected_output_data"

