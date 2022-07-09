import re
import datetime
from pyquery import PyQuery
import requests
import logging


class Tweet:
    def __init__(self, save_settings):
        self.save_settings = save_settings

    def __iter__(self):
        return iter([
            self.__dict__[field] for field in self.save_settings
            if hasattr(self, field) and self.save_settings[field]
        ])

    def to_csv(self):
        return ",".join([
            self.__dict__[field] for field in self.save_settings
            if self.save_settings[field]
        ])


def parse_page(tweetHTML, parameters, save_settings, id_origin=''):
    logger = logging.getLogger("crawler_log.parse_page")
    fh = logging.FileHandler("log.log")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    tweetPQ = PyQuery(tweetHTML)
    tweet = Tweet(save_settings)
    usernameTweet = tweetPQ("span:first.username.u-dir b").text()
    txt = re.sub(r"\s+", " ",
                 tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace(
                     '@ ', '@'))
    pic = re.findall(r"(pic.twitter.com[^\s]+)", txt)
    retweets = int(
        tweetPQ(
            "span.ProfileTweet-action--retweet span.ProfileTweet-actionCount")
        .attr("data-tweet-stat-count"))
    favorites = int(
        tweetPQ(
            "span.ProfileTweet-action--favorite span.ProfileTweet-actionCount")
        .attr("data-tweet-stat-count"))
    reply = int(
        tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount"
                ).attr("data-tweet-stat-count"))
    try:
        dateSec = int(
            tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
    except:
        logger.error('Timestamp error')
        dateSec = 0
    ids = tweetPQ.attr("data-tweet-id")
    permalink = tweetPQ.attr("data-permalink-path")

    geo = ''
    geoSpan = tweetPQ('span.Tweet-geo')
    if len(geoSpan) > 0:
        geo = geoSpan.attr('title')

    likes_users = []
    likes_url = 'https://twitter.com/i/activity/favorited_popup?id=' + str(ids)
    likes_headers = parameters['headers']
    likes_headers['Referer'] = likes_url
    likes_cookieJar = requests.cookies.RequestsCookieJar()
    try:
        likes_response = requests.get(
            (likes_url),
            cookies=likes_cookieJar,
            headers=likes_headers,
            timeout=60)
    except:
        #logger.error('Request (likes) error with code:')
        likes_users = []
    else:
        try:
            likes = PyQuery(likes_response.json()['htmlUsers'])('ol')
        except:
            pass
            #logger.error('Response without json content:' + str(likes_response.url))
        else:
            for i in likes[0]:
                likes_users.append({PyQuery(i)('div.account').attr('data-user-id') : \
                                    PyQuery(i)('div.account').attr('data-screen-name')})
    retweet_users = []
    retweet_url = 'https://twitter.com/i/activity/retweeted_popup?id=' + str(
        ids)
    retweet_headers = parameters['headers']
    retweet_headers['Referer'] = retweet_url
    retweet_cookieJar = requests.cookies.RequestsCookieJar()
    try:
        retweet_response = requests.get(
            (retweet_url),
            cookies=retweet_cookieJar,
            headers=retweet_headers,
            timeout=60)
    except:
        #logger.error('Request (retweets) error with code:')
        retweet_users = []
    else:
        try:
            retweet = PyQuery(retweet_response.json()['htmlUsers'])('ol')
        except:
            logger.error(
                'Response without json content:' + str(retweet_response.url))
        else:
            for i in retweet[0]:
                retweet_users.append({PyQuery(i)('div.account').attr('data-user-id') : \
                                    PyQuery(i)('div.account').attr('data-screen-name')})

    tweet.id_str = ids
    tweet.permalink = 'https://twitter.com' + permalink
    tweet.screenname = usernameTweet
    tweet.text = txt.encode('utf-8').decode('utf-8')
    if dateSec != 0:
        tweet.created_at = datetime.datetime.fromtimestamp(dateSec).strftime(
            "%Y-%m-%d %H:%M:%S")
    else:
        tweet.created_at = '1970-01-01'
    tweet.pic = ', '.join(pic)
    tweet.retweets = retweets
    tweet.favorites = favorites
    tweet.reply = reply
    tweet.retweet_users = retweet_users
    tweet.likes_users = likes_users
    tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
    tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))
    tweet.geo = geo
    tweet.reply_to = id_origin
    return tweet


def parse_reply(data, parameters, save_settings):
    logger = logging.getLogger("crawler_log.parse_reply")
    fh = logging.FileHandler("log.log")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    reply_refreshCursor = ''
    reply_url = 'https://twitter.com/' + data.screenname + '/status/' + data.id_str + '?conversation_id=' + data.id_str
    reply_headers = parameters['headers']
    reply_headers['Referer'] = reply_url
    reply_cookieJar = requests.cookies.RequestsCookieJar()
    main_conv_page = True
    try:
        reply_response = requests.get(
            (reply_url),
            cookies=reply_cookieJar,
            headers=reply_headers,
            timeout=60)
    except:
        logger.error('Request error with code:')
        reply_tweets = []
        reply_active = False
    else:
        reply_active = True
    counter = 0
    while reply_active:
        if main_conv_page:
            page = PyQuery(reply_response.content)
            reply_tweets = page('div.js-stream-tweet')
            reply_refreshCursor = page('div.stream-container').attr(
                'data-min-position')
            if reply_refreshCursor == '':
                reply_refreshCursor = page(
                    'li.ThreadedConversation-moreReplies').attr(
                        'data-expansion-url')
            if reply_refreshCursor is None:
                reply_refreshCursor = ''
            main_conv_page = False
        else:
            try:
                reply_json = reply_response.json()
            except:
                logger.error(
                    'Response without json content:' + str(reply_response.url))
                break

            if len(reply_json['items_html'].strip()) == 0:
                reply_active = False
                break

            reply_refreshCursor = reply_json['min_position']
            if reply_refreshCursor is None:
                reply_refreshCursor = ''
                reply_active = False
            try:
                reply_tweets = PyQuery(
                    reply_json['items_html'])('div.js-stream-tweet')
            except:
                logger.info(reply_tweets)
                logger.error('Parse error')

        for reply_tweetHTML in reply_tweets:
            reply_data = parse_page(reply_tweetHTML, parameters, save_settings,
                                    data.id_str)
            counter += 1
            if counter > 100:
                break
            yield reply_data

        reply_url = 'https://twitter.com/i/' + data.screenname + '/conversation/' + data.id_str + \
                '?conversation_id=' + data.id_str + '?include_available_features=1&include_entities=1&max_position=' + \
                reply_refreshCursor + '&reset_error_state=false'
        try:
            reply_response = requests.get(
                (reply_url),
                cookies=reply_cookieJar,
                headers=reply_headers,
                timeout=60)
        except:
            logger.error('Request error with code:')
            break


# parse json data, refresh 'page' to download new tweets
def parse(parameters,
          save_settings,
          receiveBuffer=None,
          bufferLength=100,
          proxy=None):
    logger = logging.getLogger("crawler_log.main_parse")
    fh = logging.FileHandler("log.log")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    refreshCursor = ''
    results = []
    resultsAux = []
    # if cookies in not None - it's mean repeating of the task
    if parameters['cookies'] is None:
        cookieJar = requests.cookies.RequestsCookieJar()
    else:
        cookieJar = parameters['cookies']
    active = True
    while active:
        # if any error - return current results and cookies for task manager
        try:
            response = requests.get(
                (parameters['url'] + refreshCursor),
                cookies=cookieJar,
                headers=parameters['headers'],
                timeout=60)
        except:
            # if response.status_code is not None:
            #     logger.error('Request error with code:', response.status_code)
            break

        try:
            json = response.json()
        except:
            logger.error('Response without json content:' + str(response.url))
            break

        if len(json['items_html'].strip()) == 0:
            break

        refreshCursor = json['min_position']
        tweets = PyQuery(json['items_html'])('div.js-stream-tweet')
        if len(tweets) == 0:
            break
        # parse and add to object to return
        for tweetHTML in tweets:
            out = []
            data = parse_page(tweetHTML, parameters, save_settings)
            out.append(data)
            results.append(data)
            resultsAux.append(data)
            if data.reply != 0:
                for reply_data in parse_reply(data, parameters, save_settings):
                    out.append(reply_data)
                    results.append(reply_data)
            yield out, 0, cookieJar
            if receiveBuffer and len(resultsAux) >= bufferLength:
                receiveBuffer(resultsAux)
                resultsAux = []
            # if we set limit for number of tweets
            # if parameters['maxTweets'] is not None:
            if parameters['maxTweets'] is not None:
                if 0 < parameters['maxTweets'] <= len(results):
                    active = False
                    break

    if receiveBuffer and len(resultsAux) > 0:
        receiveBuffer(resultsAux)


def parse_profile(parameters):
    logger = logging.getLogger("crawler_log.profile_parse")
    fh = logging.FileHandler("log.log")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    profile = {}
    cookieJar = requests.cookies.RequestsCookieJar()
    try:
        response = requests.get(
            (parameters['url']),
            cookies=cookieJar,
            headers=parameters['headers'],
            timeout=60)
    except:
        # logger.error('Request error :' + str(response.status_code))
        return profile, 1, cookieJar

    page = PyQuery(response.content)
    id_str = page('div.ProfileNav').attr('data-user-id')
    screenname = page('div.user-actions').attr('data-screen-name')
    name = page('div.user-actions').attr('data-name')
    tweets_number = page('li.ProfileNav-item--tweets')(
        'span.ProfileNav-value').attr('data-count')
    followers_number = page('li.ProfileNav-item--followers')(
        'span.ProfileNav-value').attr('data-count')
    following_number = page('li.ProfileNav-item--following')(
        'span.ProfileNav-value').attr('data-count')
    favorites_number = page('li.ProfileNav-item--favorites')(
        'span.ProfileNav-value').attr('data-count')
    bio = page('p.ProfileHeaderCard-bio').text()
    place = page('div.ProfileHeaderCard-location').text()
    place_id = page('span.ProfileHeaderCard-locationText')('a').attr(
        'data-place-id')
    site = page('span.ProfileHeaderCard-urlText').text()
    birth = page('span.ProfileHeaderCard-birthdateText').text()
    creation = page('span.ProfileHeaderCard-joinDateText').attr('title')
    #media_number = re.sub(r"\D+", '', page('span.PhotoRail-headingText').text())
    profile['screenname'] = screenname
    profile['id_str'] = id_str
    profile['name'] = name
    profile['tweets_number'] = tweets_number
    profile['followers_number'] = followers_number
    profile['following_number'] = following_number
    profile['favorites_number'] = favorites_number
    profile['bio'] = bio
    profile['place'] = place
    if place_id is None:
        profile['place_id'] = ''
    else:
        profile['place_id'] = place_id
    profile['site'] = site
    profile['birth'] = birth
    profile['creation'] = creation
    return profile, 0, cookieJar


# for future, return sets of date-range, like [(2016-12-12, 2016-12-24), (2016-12-24, 2016-12-31)]
def date_prepare(parameters):
    start = parameters.since
    end = parameters.until
    start = datetime.datetime.strptime(start, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')

    days_between = (end - start).days
    date_list = [(str(end - datetime.timedelta(days=x))[:10],
                  str(end - datetime.timedelta(days=(x + 1)))[:10])
                 for x in range(days_between)]
    print(date_list)
