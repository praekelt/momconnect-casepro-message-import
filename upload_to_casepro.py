import json
import logging
import requests

from settings import CASEPRO_URL, MISSED_MESSAGES


logging.basicConfig(level=logging.INFO)


def get_session():
    s = requests.Session()
    s.mount(CASEPRO_URL, requests.adapters.HTTPAdapter(max_retries=5))
    return s


def vumi_to_junebug(data):
    return {
        'to': data.get('to_addr'),
        'from': data.get('from_addr'),
        'message_id': data.get('message_id'),
        'timestamp': data.get('timestamp'),
        'content': data.get('content'),
    }


def main():
    session = get_session()
    counterrors = 0
    countsuccess = 0
    for l in open(MISSED_MESSAGES):
        data = json.loads(l)
        try:
            r = session.post(CASEPRO_URL, json=vumi_to_junebug(data))
        except Exception as e:
            logging.error('Error sending message {}. {}'.format(
                data, e.message))
            counterrors += 1
            continue
        if r.status_code < 200 or r.status_code > 299:
            logging.error(
                'Casepro response error sending message {}. '
                'Response {}'.format(data, r.text))
            counterrors += 1
            continue
        countsuccess += 1
    logging.info('Sent {} messages to casepro'.format(countsuccess))
    logging.info('Skipped {} messages that had errors'.format(counterrors))

if __name__ == '__main__':
    main()
