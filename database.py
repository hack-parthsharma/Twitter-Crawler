import csv
import sqlite3 as sql
from tqdm import tqdm
import logging


class DataBase:
    def save_tweets(self, tweets, query):
        pass

    def save_profile(self, profile):
        pass


class CsvDB(DataBase):
    def __init__(self, filename, rewrite=False):
        self.filename = filename
        self.rewrite = rewrite

    def save_tweets(self, tweets, query=None):
        mode = 'wb' if self.rewrite else 'a'
        with open(self.filename, mode=mode, encoding='utf-8') as csv_file:
            wr = csv.writer(csv_file, delimiter=',')
            for tweet in tweets:
                wr.writerow(list(tweet))


class SQLite3(DataBase):
    def __init__(self, filename):
        self.logger = logging.getLogger("crawler_log.database")
        fh = logging.FileHandler("log.log")
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d'
        )
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.table = filename
        self.db = sql.connect('.//twitter.db')
        self.cursor = self.db.cursor()

    def save_tweets(self, tweets, query):
        transaction = set((row.id_str, row.screenname, row.created_at, \
                    row.text, query['url'], row.reply_to, row.favorites, row.reply, row.retweets, \
                    ', '.join(['-'.join('{} : {}'.format(key, value) for key, value in d.items()) \
                                for d in row.likes_users]),
                    ', '.join(['-'.join('{} : {}'.format(key, value) for key, value in d.items()) \
                                for d in row.retweet_users]),
                    row.pic) for row in tweets)
        for row in transaction:
            try:
                self.cursor.execute(
                    '''INSERT INTO tweets(id_str, screenname, created_at, text, \
                    url, reply_to, favorites, replies, retweets, likes_users, retweet_users, pic)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', row)

            except:
                print('e')
                self.logger.error('Database error')
                self.db.rollback()
                self.db.commit()
        self.db.commit()

    def save_profile(self, profile, query):
        for row in [profile]:
            try:
                self.cursor.execute('''INSERT INTO profiles(id_str, screenname, name, tweets_number, followers_number, \
                    following_number, favorites_number, bio, place, place_id, site, birth, creation)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''', (row['id_str'], row['screenname'], row['name'], row['tweets_number'], row['followers_number'], \
                    row['following_number'], row['favorites_number'], row['bio'], row['place'], row['place_id'], row['site'], row['birth'], row['creation']
                ))
            except sql.IntegrityError as e:
                self.logger.error('INSERT INTO profiles error')
                self.db.rollback()
        self.db.commit()
