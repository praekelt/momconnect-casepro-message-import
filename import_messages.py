import json
import logging
import re
from uuid import UUID

from settings import EXISTING, VUMIGO_MSGS, MISSED_MESSAGES

logging.basicConfig(level=logging.INFO)


def uuid_to_int(uuid):
    return UUID(hex=uuid).int % (2147483647 + 1)


def get_casepro_existing(path):
    ids = set()
    with open(path) as f:
        for i in f:
            ids.add(int(i))
    return ids


def msg_to_data(msg):
    return {
        'from': msg['from_addr'],
        'message_id': msg['message_id'],
        'content': msg['content'],
    }


def msg_is_keyword(msg):
    first_word = msg.split()
    if len(first_word) == 0:
        return False
    first_word = first_word[0]
    first_word = re.sub(r'\W', '', first_word)
    return first_word.upper() in (
        'STOP', 'END', 'CANCEL', 'UNSUBSCRIBE', 'QUIT', 'BLOCK', 'START',
        'BABY', 'USANA', 'SANA', 'BABA', 'BABBY', 'LESEA', 'BBY', 'BABYA')


def msg_is_ussd_code(msg):
    return any((msg.find(n) > -1 for n in ('*120*', '*134*')))


def main():
    existing = get_casepro_existing(EXISTING)
    countexisting = 0
    countnew = 0
    counterrors = 0
    countkeyword = 0
    countussd = 0
    with open(VUMIGO_MSGS) as f, open(MISSED_MESSAGES, 'w') as mm:
        for i, l in enumerate(f):
            try:
                msg = json.loads(l)
            except ValueError as e:
                logging.error('Cannot decode message on line {}: {}'.format(
                    i, e.message))
                counterrors += 1
                continue

            try:
                data = msg_to_data(msg)
            except KeyError as e:
                logging.error(
                    'Invalid message on line {}: missing field {}. {}'.format(
                        i, e.message, msg))
                counterrors += 1
                continue

            if uuid_to_int(data['message_id']) in existing:
                logging.info(
                    'Message already in casepro, skipping. {}'.format(data))
                countexisting += 1
            elif msg_is_keyword(data['content']):
                logging.info(
                    'Message is a keyword message, skipping. {}'.format(data))
                countkeyword += 1
            elif msg_is_ussd_code(data['content']):
                logging.info(
                    'Message is a ussd number, skipping. {}'.format(data))
                countussd += 1
            else:
                mm.write(l)
                logging.info('Not in casepro: {}'.format(data))
                countnew += 1
    mm.close()
    logging.info('{} new messages not in casepro'.format(countnew))
    logging.info('Skipped {} existing messages in casepro'.format(
        countexisting))
    logging.info('Skipped {} keyword messages'.format(countkeyword))
    logging.info('Skipped {} ussd messages'.format(countussd))
    logging.info('Skipped {} messages that had errors'.format(counterrors))

if __name__ == '__main__':
    main()
