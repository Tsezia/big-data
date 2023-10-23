from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, TextValueProtocol

MRJob.INPUT_PROTOCOL = JSONValueProtocol
MRJob.OUTPUT_PROTOCOL = TextValueProtocol

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        for record in line:
            yield record['lecturer_title'] or record['lecturer'], 1

    def reducer(self, key, values):
        yield key, (key or "--") + "\t " + str(sum(values))


if __name__ == '__main__':
    MRWordFrequencyCount.run()