import feedparser
import sqlite3
from bs4 import BeautifulSoup
import twitter
import json
import time
import argparse
import os

ENTRY_IN_MEMORY = 500

class Bot():
    def __init__(self, database='bot.db'):
        self.db = database
        with sqlite3.connect(database) as db:
            db.execute('CREATE TABLE IF NOT EXISTS `feeds` \
                        (`url` TEXT NOT NULL UNIQUE, \
                         `etag` TEXT, \
                         `modified` REAL DEFAULT 0, \
                         PRIMARY KEY(`url`));')
            db.execute('CREATE TABLE IF NOT EXISTS `entries`  \
                        (`feed` INTEGER, \
                         `title` TEXT NOT NULL, \
                         `link` TEXT, \
                         `image` TEXT, \
                         `date` REAL NOT NULL, \
                         `published` INTEGER, \
                         PRIMARY KEY(`title`));')
            db.execute('CREATE TABLE IF NOT EXISTS `settings` \
                        (`category` TEXT, \
                         `key` TEXT NOT NULL UNIQUE, \
                         `value` TEXT NOT NULL, \
                         PRIMARY KEY(`key`));')

    def add_feed(self, url):
        with sqlite3.connect(self.db) as db:
            db.execute('INSERT OR IGNORE INTO feeds(`url`) VALUES(?);', (url.strip().lower(),))

    def remove_feed(self, url):
        with sqlite3.connect(self.db) as db:
            (row_id,) = db.execute('SELECT _rowid_ FROM `feeds` WHERE `url` = ?', (url.strip().lower(),)).fetchone()
            db.execute('DELETE FROM `feeds` WHERE `url` = ?;', (url.strip().lower(),))
            db.execute('DELETE FROM `entries` WHERE `feed` = ?;', (row_id,))

    def get_new_entries(self):
        with sqlite3.connect(self.db) as db:
            feeds = db.execute('SELECT _rowid_, `url`, `etag`, `modified` FROM `feeds`;').fetchall()
            for (row_id, url, etag, modified) in feeds:
                d = feedparser.parse(url, etag=etag, modified=time.localtime(modified))
                if d.status == 200:
                    if hasattr(d, 'etag'):
                        db.execute('UPDATE `feeds` SET `etag`=? WHERE `url`=?;', (d.etag, url))
                    if hasattr(d, 'modified'):
                        db.execute('UPDATE `feeds` SET `modified`=? WHERE `url`=?;',
                                    (time.mktime(d.modified_parsed), url))
                    entries = set(db.execute('SELECT `title` FROM `entries` WHERE `feed` = ?;', (row_id,)).fetchall())
                    for entry in d.entries[0:ENTRY_IN_MEMORY]:
                        title = entry.title
                        if title not in entries:
                            link = entry.link
                            date = time.mktime(entry.published_parsed)
                            soup = BeautifulSoup(entry.summary, 'html.parser')
                            image = soup.find_all('img', limit=1)
                            if len(image) > 0:
                                image = image[0].get('src')
                            else:
                                image = None
                            db.execute('INSERT OR IGNORE INTO `entries` (`feed`, `title`, `link`, `date`, `image`, \
                                       `published`) VALUES (?, ?, ?, ?, ?, ?);', (row_id, title, link, date, image, 0))
                    db.commit()

    def get_settings(self, category):
        result = {}
        with sqlite3.connect(self.db) as db:
            for (key, value) in db.execute('SELECT `key`, `value` FROM `settings` \
                                            WHERE `category` = ?;', (category,)).fetchall():
                result[key] = value
        return result

    def publish_on_twitter(self):
        credentials = self.get_settings('twitter')
        api = twitter.Api(consumer_key=credentials['consumer_key'],
                          consumer_secret=credentials['consumer_secret'],
                          access_token_key=credentials['access_token_key'],
                          access_token_secret=credentials['access_token_secret'])
        with sqlite3.connect(self.db) as db:

            unpublished = db.execute('SELECT _rowid_, `title`, `link`, `image` FROM `entries` WHERE `published` = 0;')
            for (id, title, link, image) in unpublished:
                result = api.PostUpdate('{} {}'.format(title, link), media=image)
                if result:
                    db.execute('UPDATE `entries` SET `published` = 1 WHERE _rowid_ = ?;', (id,))

    def import_twitter_credentials(self, fname):
        credentials = json.loads(open(fname, 'r').read())
        with sqlite3.connect(self.db) as db:
            for key, value in credentials.items():
                db.execute('INSERT OR REPLACE INTO `settings` (`category`, `key`, `value`) \
                            VALUES (?, ?, ?);', ('twitter', key, str(value)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Twitter bot parsing RSS feeds and posting news to Twitter')
    parser.add_argument('--db', default='twitter-bot.db', dest='db')
    parser.add_argument('action', nargs='+', help='Action to be performed')
    args = parser.parse_args()
    bot = Bot(database=args.db)
    if args.action[0] == 'update':
        bot.get_new_entries()
    elif args.action[0] == 'publish':
        bot.publish_on_twitter()
    elif args.action[0] == 'import-credentials' and len(args.action) > 1:
        if os.path.isfile(args.action[1]):
            bot.import_twitter_credentials(args.action[1])
    elif args.action[0] == 'add' and len(args.action) > 1:
        bot.add_feed(args.action[1])
    elif args.action[0] == 'remove' and len(args.action) > 1:
        bot.remove_feed(args.action[1])


