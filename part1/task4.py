from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, TextValueProtocol

MRJob.INPUT_PROTOCOL = JSONValueProtocol
MRJob.OUTPUT_PROTOCOL = TextValueProtocol

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        # doc = json.loads(line)
        for record in line:
            yield (record['lecturer_title'] or record['lecturer'], record['kindOfWork'] or '--'), 1

    def reducer(self, key, values):
        yield key, "\t ".join(key) + "\t " + str(sum(values))


if __name__ == '__main__':
    MRWordFrequencyCount.run()