import argparse
import collections

from pyspark import SparkConf, SparkContext


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute the histogram of rating values from the MovieLens 100K dataset."
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default="file:///opt/spark/work-dir/ml-100k/u.data",
        help="Path to the ratings data file (default: %(default)s)",
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master URL to use (default: %(default)s)",
    )
    parser.add_argument(
        "--app-name",
        default="RatingsHistogram",
        help="Name to assign to the Spark application (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    conf = SparkConf().setMaster(args.master).setAppName(args.app_name)
    sc = SparkContext(conf=conf)

    try:
        lines = sc.textFile(args.input_path)
        ratings = lines.map(lambda x: x.split()[2])
        result = ratings.countByValue()

        sorted_results = collections.OrderedDict(sorted(result.items()))
        for key, value in sorted_results.items():
            print(f"{key} {value}")
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
