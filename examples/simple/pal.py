"""
palindrome worker.
"""

from pipeline.worker import ListConsumerWorker, run

class Pal(ListConsumerWorker):
    """
    takes lists of ints and outputs palindromes
    """

    def process_item(self, item):
        istr = str(item)
        rev = ''.join(reversed(istr))
        return int(istr + rev)

run(Pal, "simple-pal", "palindrome worker for simple pipeline")
