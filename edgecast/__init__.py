import arrow
import json
import requests


class EdgeCastError(Exception):
    pass


class Client(object):

  def __init__(self, account_id, token):
    self.account_id = account_id
    self.token = token

    self._url = 'https://api.edgecast.com/v2'
    self._session = requests.Session()


  def _request(self, endpoint, method='GET', data=None, **kwargs):
    kwargs.update({'AccountID': self.account_id})
    _endpoint = endpoint % kwargs
    _url = '%(url)s/%(endpoint)s' % {'url':self._url, 'endpoint': _endpoint }

    response = self._session.request(
      method,
      _url,
      headers={
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'TOK:%s' % (self.token),
      },
      data=json.dumps(data)
    )

    try:
      response.raise_for_status()
    except:
      raise EdgeCastError('unexpected response code %d: %s' % (
        response.status_code,
        response.text
        )
      )

    return response.json()


  def EdgeNodes(self):
    return self._request('mcc/customers/%(AccountID)s/edgenodes')


  def BytesTransferred(self, start_date_time, end_date_time, pops=None, region_id=None):
    _endpoint = 'reporting/customers/%(AccountID)s/bytestransferred?begindate=' \
      '%(StartDateTime)s&enddate=%(EndDateTime)s'
    if pops:
      _endpoint = "".join((_endpoint, '&pops=%(POPs)s'))
    if region_id:
      _endpoint = "".join((_endpoint, '&regionid=%(RegionID)s'))

    return self._request(_endpoint, StartDateTime=start_date_time, \
      EndDateTime=end_date_time, POPs=pops, RegionID=region_id
    )


  def CustomReport(self, platform, start_date, end_date, metric_code='Hits', group_code='HTTP_STATUS'):
    _endpoint = 'reporting/customers/%(AccountID)s/media/%(Platform)s/customreport' \
      '?begindate=%(StartDate)s&enddate=%(EndDate)s&metriccode=%(MetricCode)s&groupcode=%(GroupCode)s'
    return self._request(_endpoint, Platform=platform, StartDate=start_date, EndDate=end_date, \
      MetricCode=metric_code, GroupCode=group_code
    )


  def CnameReportCodes(self, platform, start_date, end_date):
    _endpoint = 'reporting/customers/%(AccountID)s/media/%(Platform)s/cnamereportcodes' \
      '?begindate=%(StartDate)s&enddate=%(EndDate)s'
    return self._request(_endpoint, Platform=platform, StartDate=start_date, EndDate=end_date)


class EdgecastReportReader(object):

  def __init__(self, config, start, end, kind='region', platform=None, granularity='day'):
    self.account_id = config['account_id']
    self.token = config['token']
    self.start = arrow.get(start, 'YYYYMMDD')
    self.end = arrow.get(end, 'YYYYMMDD').ceil('day')
    self.kind = kind
    self.platform = platform
    self.granularity = granularity
    print 'Processing {} report from {} to {} with {} interval'.format(self.kind, self.start, self.end, self.granularity)

  def __iter__(self):
    self.client = Client(self.account_id, self.token)
    if self.kind == 'region':
      for start, end in arrow.Arrow.span_range(self.granularity, self.start, self.end):
        for node in self.client.EdgeNodes():
          response = self.client.BytesTransferred(start.format('YYYY-MM-DDTHH:mm:ss'), end.format('YYYY-MM-DDTHH:mm:ss'), node['Code'])
          response.update(node)
          response.update({'TimeInterval': '{start}/{end}'.format(start=start, end=end)})
          yield response

    if self.kind == 'cname':
      for start, end in arrow.Arrow.span_range(self.granularity, self.start, self.end):
        for response in self.client.CnameReportCodes(self.platform, start.format('YYYY-MM-DDTHH:mm:ss'), end.format('YYYY-MM-DDTHH:mm:ss')):
          response.update({'TimeInterval': '{start}/{end}'.format(start=start, end=end)})
          yield response

    if self.kind == 'custom':
      for start, end in arrow.Arrow.span_range(self.granularity, self.start, self.end):
        for response in self.client.CustomReport(self.platform, start.format('YYYY-MM-DDTHH:mm:ss'), end.format('YYYY-MM-DDTHH:mm:ss')):
          for distributions in response['Data']:
            for distribution in distributions['Data']:
              distribution.update({'TimeInterval': '{start}/{end}'.format(start=start, end=end)})
              yield distribution
