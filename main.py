import argparse

from src.service.consume import consume, postgre, sample

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="reprocess data IMA")
    parser.add_argument("--args", nargs='+', default=[0])
    args = parser.parse_args()
    postgre(int(vars(args).get('args')[0]))
    # sample()
    # consume()


