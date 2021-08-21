"""
Measures user perceived update latency of Cirrus indices

General design:

Input pages come from the rc feed.
Every N seconds copy an event from the rc feed to the queue to check
For all items in q poll mw api for current revision id in search
Report latency difference between rc_timestamp and when we see the rev_id

Non-goals:

Mesuring latencies < 60s or so. Elastic only refreshes the indices
every 30s anyways, trying to clamp down on latency isn't worthwhile.

"""

from argparse import ArgumentParser
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
import json
import requests
from sseclient import SSEClient as EventSource
import sys
import time
from typing import Callable, Mapping, Sequence, Tuple
from threading import Thread

user_agent = "cirrussearch-update-lag-checker bot by User:So9q"
header = {
    "User-Agent": user_agent
}


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    return parser


def make_input_queue(
        event_url: str,
        filter_fn: Callable[[str], bool] = lambda x: True,
        sleep: Callable[[], None] = lambda: time.sleep(10),
) -> deque:
    # Deque will drop items off the left as we add to the right, always
    # consume from the right (pop) for most recent events.
    event_q = deque(maxlen=5)

    def feed_event_q():
        for event in EventSource(event_url):
            if event.event == 'message' and filter_fn(event.data):
                event_q.append(event.data)

    Thread(target=feed_event_q, daemon=True).start()

    # To ensure fair sampling we take one event from the event stream
    # every n seconds
    final_q = deque(maxlen=5)

    def feed_final_q():
        while True:
            try:
                item = event_q.pop()
            except IndexError:
                pass
            else:
                final_q.append(item)
            sleep()

    Thread(target=feed_final_q, daemon=True).start()

    return final_q


def cirrusdump(titles: Sequence[str]) -> Sequence[Mapping]:
    titles = '|'.join(titles)
    if titles == '':
        return {}

    res = requests.get('https://www.wikidata.org/w/api.php', params={
        'action': 'query',
        'prop': 'cirrusdoc',
        'titles': titles,
        'format': 'json',
        'formatversion': 1,
    }, headers=header)
    res.raise_for_status()
    # All requested pages are returned, those not found are flagged missing
    # and giving negative page_ids.
    return res.json()['query']['pages']


def current_search_rev(titles: Sequence[str]) -> Sequence[Tuple[str, int]]:
    for page in cirrusdump(titles).values():
        rev_ids = [doc['source']['version'] for doc in page.get('cirrusdoc', [])]
        try:
            rev_id = max(rev_ids)
        except ValueError:
            rev_id = None
        yield (page['title'], rev_id)


@dataclass
class Revision:
    page_title: str
    rev_id: int
    rc_timestamp: datetime


def monitor_latency(q, clock=time.time, sleep=lambda: time.sleep(10)):
    revs = {}

    def fill_revs():
        # Fill revs with anything waiting for us in the input queue
        while True:
            try:
                raw_event = q.pop()
            except IndexError:
                return

            event = json.loads(raw_event)
            rev = Revision(
                page_title=event['title'],
                rev_id=event['revision']['new'],
                rc_timestamp=event['timestamp']
            )
            # TODO: What about overwrites?
            revs[rev.page_title] = rev

    while True:
        fill_revs()
        print('Checking {} pages'.format(len(revs)))
        # TODO: Check for stale revs, age them out eventually.
        for page_title, cur_rev_id in current_search_rev(revs.keys()):
            rev = revs[page_title]
            if cur_rev_id and cur_rev_id >= rev.rev_id:
                del revs[page_title]
                yield rev, clock()
        sleep()


def main(
        event_url: str = 'https://stream.wikimedia.org/v2/stream/recentchange',
):
    def input_filter(event_data):
        # Takes plain string input, there are a lot of events and parsing all that
        # json is expensive. This is probably fragile, but we are only sampling anyways.
        is_wikidata = '"domain":"www.wikidata.org"' in event_data
        is_edit = '"type":"edit"' in event_data
        is_new = '"type":"new"' in event_data

        return is_wikidata and (is_edit or is_new)

    q = make_input_queue(
        event_url,
        filter_fn=input_filter)

    for rev, found_at in monitor_latency(q):
        lag = found_at - rev.rc_timestamp
        print('{} - {:.0f}s'.format(rev.page_title, lag))


if __name__ == "__main__":
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))