from collections import Counter
import dateutil.parser
import json
import logging

from settings import MISSED_MESSAGES

logging.basicConfig(level=logging.INFO)


def main():
    dates = Counter()
    for l in open(MISSED_MESSAGES):
        msg = json.loads(l)
        if 'timestamp' not in msg:
            continue
        timestamp = dateutil.parser.parse(msg['timestamp'])
        dates[timestamp.date()] += 1
    for d in sorted(dates):
        logging.info('{}: {}'.format(d, dates[d]))

if __name__ == '__main__':
    main()
