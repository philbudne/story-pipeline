"""
data sink: outputs list items
"""

from pipeline.worker import ListConsumerWorker, run

class Out(ListConsumerWorker):
    """
    takes lists of ints and prints them.
    """

    def process_item(self, item):
        print("out:", item)
        return None

run(Out, "simple-out", "output worker for simple pipeline")
