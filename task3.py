from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol, TextValueProtocol

MRJob.INPUT_PROTOCOL = JSONValueProtocol
MRJob.OUTPUT_PROTOCOL = TextValueProtocol

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_pre,
                   reducer=self.reducer_pre),
            MRStep(reducer=self.reducer_find_max)
        ]

    def mapper_pre(self, _, line):
        # doc = json.loads(line)
        for record in line:
            yield ((record['lecturer_title'] or record['lecturer']), record['auditorium']), 1

    def reducer_pre(self, key, values):
        yield key[0], (sum(values), key)

    def reducer_find_max(self, _, values):
        yield '--',  str('\t'.join(max(values)[1]))


if __name__ == '__main__':
    MRWordFrequencyCount.run()