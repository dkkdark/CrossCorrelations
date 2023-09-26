import sys
from abc import ABC
from mrjob.job import MRJob
from mrjob.step import MRStep
from pyhdfs import HdfsClient
from collections import defaultdict

from input_generator import generate_input


class CrossCorrelationPairs(MRJob, ABC):

    def configure_args(self):
        super(CrossCorrelationPairs, self).configure_args()
        self.add_passthru_arg('--stage', type=int, required=True)

    def steps(self):
        if self.options.stage == 1:
            return [
                MRStep(mapper=self.mapper_stage1, reducer=self.reducer_stage1)
            ]
        elif self.options.stage == 2:
            return [
                MRStep(mapper=self.mapper_stage2, reducer=self.reducer_stage2)
            ]

    def mapper_stage1(self, _, line):
        items = line.strip().split()
        for item1 in items:
            for item2 in items:
                if item1 != item2:
                    yield [item1, item2], 1

    def reducer_stage1(self, key, values):
        yield key, sum(values)

    def mapper_stage2(self, _, line):
        items = line.strip().split()
        for i in range(len(items)):
            H = defaultdict(int)
            for j in range(len(items)):
                if j != i:
                    H[items[j]] += 1
            yield items[i], H

    def reducer_stage2(self, item, stripes):
        merged_stripe = defaultdict(int)
        for stripe in stripes:
            for key, value in stripe.items():
                merged_stripe[key] += value
        yield item, merged_stripe

    def send_to_hdfs(self):
        hdfs = HdfsClient(hosts='localhost:50070', user_name='blohinaksenia')
        if self.options.stage == 1:
            if hdfs.exists("/output_pairs.txt"):
                hdfs.delete("/output_pairs.txt")
            hdfs.copy_from_local("/home/blohinaksenia/crossCorelation/output_pairs.txt", "/output_pairs.txt")
        if self.options.stage == 2:
            if hdfs.exists("/output_stripes.txt"):
                hdfs.delete("/output_stripes.txt")
            hdfs.copy_from_local("/home/blohinaksenia/crossCorelation/output_stripes.txt", "/output_stripes.txt")


def read_data():
    data = {}

    hdfs = HdfsClient(hosts='localhost:50070', user_name='blohinaksenia')

    with hdfs.open("/output_stripes.txt") as f:
        data_file = f.read().decode('utf-8').split('\n')
        for line in data_file:
            line = line.strip()
            if line:
                parts = line.split('\t')
                if len(parts) == 2:
                    product = parts[0].strip('"')
                    related_data = eval(parts[1])
                    data[product] = related_data

    return data


def get_top_recommendations(product_name):
    data = read_data()

    recommendations = {}
    if product_name in data:
        product_data = data[product_name]
        for related_product, count in product_data.items():
            recommendations[related_product] = count
    sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
    return sorted_recommendations[:10]


if __name__ == '__main__':
    if len(sys.argv) == 3:
        command = sys.argv[1]
        arg = sys.argv[2]
        if command == "recommend":
            top = get_top_recommendations(arg)
            print("\nТоп рекомендации: ")
            print(top)
        if command == "generate":
            generate_input(int(arg))
    else:
        cor = CrossCorrelationPairs()
        cor.run()
        cor.send_to_hdfs()
