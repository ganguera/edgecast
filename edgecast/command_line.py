import argparse
import arrow
import json
import config
from . import EdgecastReportReader
from media_type import PLATFORM


def main():

  parser = argparse.ArgumentParser(
    description='EdgeCast Usage Report Reader'
  )

  parser.add_argument('-g', '--granularity',
    dest='granularity', action='store', type=str,
    default='day', choices=['hour', 'day'],
    help='Size in which data fields are sub-divided'
  )

  parser.add_argument('-r', '--range',
    dest='date_range', action='store', type=str,
    default='{yesterday}:{yesterday}'.format(yesterday=arrow.utcnow().shift(days=-1).format('YYYYMMDD')),
    help='Date range to show. YYYYMMDD:YYYYMMDD'
  )

  parser.add_argument('-t', '--type',
    dest='type', action='store', type=str, default='region',
    choices=['region', 'cname', 'custom'], help='Report Type'
  )

  parser.add_argument('-p', '--platform',
    dest='platform', action='store', type=str, default='HTTP_SMALL',
    choices=PLATFORM, help='Media Type Platform'
  )

  args = parser.parse_args()

  start, end = args.date_range.split(':')
  platform = PLATFORM[args.platform]

  reader = EdgecastReportReader(config.input['edgecast'], start, end, args.type, platform, args.granularity)

  for read in reader:
    print json.dumps(read)
