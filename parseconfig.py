# argparse
import argparse

def parse_config(config_file):
    with open(config_file, 'r') as f:
        config = f.readlines()
        for line in config:
            print(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', help='config file')

    args = parser.parse_args()
    config_file = args.c
    parse_config(config_file)