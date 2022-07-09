from requests.utils import quote
import json
import itertools
import datetime
from fake_useragent import UserAgent


def get_headers():
    ua = UserAgent()
    headers = {
        'Host':
        "twitter.com",
        'User-Agent':
        ua.random,
        #'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0",
        'Accept':
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        'Accept-Language':
        "en-US,en;q=0.5",
        'Connection':
        "keep-alive"
    }

    return headers


def create_tweet_query(screen_name=None,
                       maxTweets=None,
                       since=None,
                       until=None,
                       querySearch='',
                       topTweets=False,
                       near=None,
                       within=None,
                       cookies=None):
    parameters_url = (('', querySearch), (' from:', screen_name),
                      (' near:', near), (' within:', within), (' since:',
                                                               since),
                      (' until:', until), ('&src=typd&max_position=', ''))

    parameters_api = {
        'screen_name': screen_name,
        'maxTweets': maxTweets,
        'topTweets': topTweets,
        'cookies': cookies
    }
    if parameters_url[0][1] != '':
        url = 'https://twitter.com/i/search/timeline?l=&q='
    else:
        url = 'https://twitter.com/i/search/timeline?l=&q='

    for i in parameters_url:
        if i[1] is not None:
            url += i[0] + i[1]

    maxTweets = maxTweets
    topTweets = topTweets
    cookieJar = cookies

    query = {
        'headers': get_headers(),
        'url': url,
        'maxTweets': maxTweets,
        'topTweets': topTweets,
        'cookies': cookieJar
    }

    return query


def create_task(query, type, recursion=0, saveParam=None):
    return json.dumps({
        'query_param': query,
        'save_param': saveParam,
        'type': type,
        'recursion': recursion
    })


def create_profile_query(screenname):
    return {
        'headers': get_headers(),
        'url': 'https://twitter.com/' + screenname
    }


def parse_location(geo):
    if geo is None:
        return None
    lon = geo.get("lon", 0)
    lat = geo.get("lat", 0)
    city = geo.get("city", '')
    country = geo.get("country", '')
    return ",".join([city, country]) \
        if "city" in geo or "country" in geo \
        else ",".join([str(lon), str(lat)])


def date_range(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta
    yield end


def create_profile_tasks(profiles):
    profile_tasks = []
    for prof in profiles:
        profile_query = create_profile_query(screenname=prof)
        profile_tasks.append(create_task(query=profile_query, type='profile'))
    return profile_tasks


def create_tasks(queries, saveParam, days_interval=3):
    tweet_tasks = []
    profiles = []
    for q in queries:
        maxTweets = q.get('maxTweets', None)
        topTweets = q.get('topTweets', None)
        recursion = q.get('recursion', 0)

        a = []
        if 'querySearch' in q:
            a.append([('querySearch', geo) for geo in q['querySearch']])
        if 'locations' in q:
            a.append([('geo', geo) for geo in q['locations']])

        if 'screen_name' in q:
            a.append([('screen_name', name) for name in q['screen_name']])
            profiles += q['screen_name']

        now = datetime.datetime.today()
        since = datetime.datetime.strptime(q['since'], '%Y-%m-%d') \
            if ('since' in q) \
            else now - datetime.timedelta(weeks=4 * 6)

        until = datetime.datetime.strptime(q['until'], '%Y-%m-%d') \
            if ('until' in q) \
            else now

        dates = list(
            map(lambda d: str(d.date()),
                date_range(
                    since, until, datetime.timedelta(days=days_interval))))
        intervals = [('interval', (d1, d2))
                     for d1, d2 in zip(dates[0::2], dates[1::2])]
        a.append(intervals)

        for element in itertools.product(*a):
            p = dict([(a, b) for a, b in element])
            query = create_tweet_query(
                screen_name=p.get('screen_name'),
                near=parse_location(p.get('geo')),
                within=str(p['geo']['radius']) + 'km' if 'geo' in p else None,
                maxTweets=maxTweets,
                since=p.get('interval')[0],
                until=p.get('interval')[1],
                querySearch=p['querySearch'] if 'querySearch' in p else '',
                topTweets=topTweets)
            tweet_tasks.append(
                create_task(
                    query=query,
                    saveParam=saveParam,
                    type='tweets',
                    recursion=recursion))

    return tweet_tasks + create_profile_tasks(set(profiles))
