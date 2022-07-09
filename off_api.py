import csv
import tweepy


class Tweet:
    def __init__(self):
        pass


def login():
    auths = []
    keys = [[
        'EuWfHYw3IobAL8q2xrhmRQaz6',
        'flVDfdM9ClclgCtkiOUGzKZzH5KjsvnkOGwcxJEUu18OET1wQL'
    ]]

    for key in keys:
        auth = tweepy.AppAuthHandler(key[0], key[1])
        auths.append(auth)

    api = tweepy.API(
        auths[0], wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def get_tweets_3200(api, parameters):

    print('began get tweets')
    #init list for tweets
    all_tweets = []
    name = parameters[0]['screen_name']
    print(name)
    #first request, we get last id to next iter
    try:
        new_tw = api.user_timeline(screen_name=name, count=1)
    except tweepy.TweepError as err:
        return all_tweets, err

    all_tweets.extend(new_tw)
    last_id = all_tweets[-1].id - 1

    while len(new_tw) > 0 and parameters['maxTweets'] > len(all_tweets):
        try:
            new_tw = api.user_timeline(
                screen_name=name, count=200, max_id=last_id)
        except tweepy.TweepError as err:
            return all_tweets, err

        all_tweets.extend(new_tw)

        last_id = all_tweets[-1].id - 1

    return all_tweets, 0


def get_followers(api, parameters):
    print('began get fol-rs1')
    name = parameters[0]['screen_name']

    #init cursor obj for follower
    try:
        followers = tweepy.Cursor(api.followers_ids, id=name)
    except tweepy.TweepError as err:
        return [], err

    ids = []
    foll_names = []

    #cursor works with pages, so
    for page in followers.pages():
        ids.extend(page)

    #now we need in screen name, lookup works only with 100 and less len of list
    #that is why we use few loop - to increase efficient
    id_prepare = group(ids, 99)

    for part in id_prepare:
        try:
            users_prepare = api.lookup_users(part)
        except tweepy.TweepError as err:
            return ids, err

        foll_names.extend([us._json['screen_name'] for us in users_prepare])

    return ids, 0


def get_following(api, parameters):
    print('began get fol-ing')
    name = parameters[0]['screen_name']
    #init cursor obj for following
    try:
        following = tweepy.Cursor(api.friends_ids, id=name)
    except tweepy.TweepError as err:
        return [], err

    ids = []
    friend_names = []
    #cursor works with pages, so
    for page in following.pages():
        ids.extend(page)

    id_prepare = group(ids, 99)

    for part in id_prepare:
        try:
            users_prepare = api.lookup_users(part)
        except tweepy.TweepError as err:
            return ids, err

        friend_names.extend([us._json['screen_name'] for us in users_prepare])

    return ids, 0


def group(iterable, count):
    return [iterable[i:i + count] for i in range(0, len(iterable), count)]


def get_number_of_tweets(api, parameters):
    #ooookay, it's boring
    return 0
