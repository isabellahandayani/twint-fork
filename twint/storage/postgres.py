import json
import sys
import time
import hashlib
import psycopg2
import logging as logme
from datetime import datetime

def ConnPostgres(database):
    if database:
        print("[+] Inserting into DatabasePostgres: " + str(database))
        conn = init(database)
        print("DONE")
        if isinstance(conn, str): # error
            sys.exit(1)
    else:
        logme.error("FAILED CONNECTIOn")
        conn = ""

    return conn

def init(db):
    try:
        conn = psycopg2.connect(**db)
        conn.autocommit = True
        cursor = conn.cursor()

        table_users = """
            CREATE TABLE IF NOT EXISTS
                twitterusers(
                    id bigint not null UNIQUE,
                    id_str text not null,
                    name text,
                    username text not null,
                    bio text,
                    location text,
                    url text,
                    join_date text not null,
                    join_time text not null,
                    tweets bigint,
                    following bigint,
                    followers bigint,
                    likes bigint,
                    media bigint,
                    private bool not null,
                    verified bool not null,
                    profile_image_url text not null,
                    background_image text,
                    hex_dig text not null,
                    time_update bigint not null,
                    CONSTRAINT twitterusers_pk PRIMARY KEY (id, hex_dig)
                );
            """
        cursor.execute(table_users)

        table_tweets = """
            CREATE TABLE IF NOT EXISTS
                tweets (
                    id bigint not null UNIQUE,
                    id_str text not null,
                    tweet text default '',
                    language text default '',
                    conversation_id text not null,
                    created_at text not null,
                    date text not null,
                    time text not null,
                    timezone text not null,
                    place text default '',
                    replies_count bigint,
                    likes_count bigint,
                    retweets_count bigint,
                    user_id bigint not null,
                    user_id_str text not null,
                    screen_name text not null,
                    name text default '',
                    link text,
                    mentions jsonb,
                    hashtags text array,
                    cashtags text array,
                    urls text array,
                    photos text array,
                    thumbnail text,
                    quote_url text,
                    video bigint,
                    geo text,
                    near text,
                    source text,
                    time_update bigint not null,
                    translate text default '',
                    trans_src text default '',
                    trans_dest text default '',
                    additional_id int not null default -1,
                    additional_attribute text default 'undefined' not null,
                    additional_attribute2 text default 'undefined' not null,
                    additional_attribute3 text default 'undefined' not null,
                    additional_attribute4 text default 'undefined' not null,
                    additional_attribute5 text default 'undefined' not null,
                    CONSTRAINT tweets_pk PRIMARY KEY (id)
                );
        """
        cursor.execute(table_tweets)

        table_retweets = """
            CREATE TABLE IF NOT EXISTS
                retweets(
                    user_id bigint not null,
                    username text not null,
                    tweet_id bigint not null,
                    retweet_id bigint not null,
                    retweet_date bigint,
                    CONSTRAINT retweets_pk PRIMARY KEY(user_id, tweet_id),
                    CONSTRAINT tweet_id_fk FOREIGN KEY(tweet_id) REFERENCES tweets(id)
                );
        """
        cursor.execute(table_retweets)

        table_reply_to = """
            CREATE TABLE IF NOT EXISTS
                replies(
                    tweet_id bigint not null,
                    user_id bigint not null,
                    username text not null,
                    CONSTRAINT replies_pk PRIMARY KEY (user_id, tweet_id),
                    CONSTRAINT tweet_id_fk FOREIGN KEY (tweet_id) REFERENCES tweets(id)
                );
        """
        cursor.execute(table_reply_to)

        table_favorites =  """
            CREATE TABLE IF NOT EXISTS
                favorites(
                    user_id bigint not null,
                    tweet_id bigint not null,
                    CONSTRAINT favorites_pk PRIMARY KEY (user_id, tweet_id),
                    CONSTRAINT tweet_id_fk FOREIGN KEY (tweet_id) REFERENCES tweets(id)
                );
        """
        cursor.execute(table_favorites)

        # table_followers = """
        #     CREATE TABLE IF NOT EXISTS
        #         followers (
        #             id bigint not null,
        #             follower_id bigint not null,
        #             CONSTRAINT followers_pk PRIMARY KEY (id, follower_id),
        #             CONSTRAINT id_fk FOREIGN KEY(id) REFERENCES users(id),
        #             CONSTRAINT follower_id_fk FOREIGN KEY(follower_id) REFERENCES users(id)
        #         );
        # """
        # cursor.execute(table_followers)

        # table_following = """
        #     CREATE TABLE IF NOT EXISTS
        #         following (
        #             id bigint not null,
        #             following_id bigint not null,
        #             CONSTRAINT following_pk PRIMARY KEY (id, following_id),
        #             CONSTRAINT id_fk FOREIGN KEY(id) REFERENCES users(id),
        #             CONSTRAINT following_id_fk FOREIGN KEY(following_id) REFERENCES users(id)
        #         );
        # """
        # cursor.execute(table_following)

        # table_followers_names = """
        #     CREATE TABLE IF NOT EXISTS
        #         followers_names (
        #             username text not null,
        #             time_update bigint not null,
        #             follower text not null,
        #             CONSTRAINT followers_name_user_pk PRIMARY KEY (username, follower)
        #         );
        # """
        # cursor.execute(table_followers_names)

        # table_following_names = """
        #     CREATE TABLE IF NOT EXISTS
        #         following_names (
        #             username text not null,
        #             time_update bigint not null,
        #             follows text not null,
        #             PRIMARY KEY (username, follows)
        #         );
        # """
        # cursor.execute(table_following_names)
        return conn
    except Exception as e:
        return str(e)

def fTable(Followers):
    if Followers:
        table = "followers_names"
    else:
        table = "following_names"

    return table

def uTable(Followers):
    if Followers:
        table = "followers"
    else:
        table = "following"

    return table

def follow(conn, Username, Followers, User):
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        entry = (User, time_ms, Username,)
        table = fTable(Followers)
        query = f"INSERT INTO {table} VALUES(?,?,?)"
        cursor.execute(query, entry)
        conn.commit()
    except psycopg2.IntegrityError:
        pass

def get_hash_id(conn, id):
    cursor = conn.cursor()
    id = int(id)
    cursor.execute(f"SELECT hex_dig FROM twitterusers WHERE id = {id} LIMIT 1")
    resultset = cursor.fetchall()
    return resultset[0][0] if resultset else -1

def user(conn, config, User):
    try:
        time_ms = round(time.time()*1000)
        logme.debug(__name__ + ':Postgres:user:'+ str(conn))
        cursor = conn.cursor()
        user = [int(User.id), User.id, User.name, User.username, User.bio, User.location, User.url,User.join_date, User.join_time, User.tweets, User.following, User.followers, User.likes, User.media_count, User.is_private, User.is_verified, User.avatar, User.background_image]

        hex_dig = hashlib.sha256(','.join(str(v) for v in user).encode()).hexdigest()
        entry = tuple(user) + (hex_dig,time_ms,)
        old_hash = get_hash_id(conn, User.id)

        if old_hash == -1 or old_hash != hex_dig:
            query = 'INSERT INTO twitterusers VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cursor.execute(query, entry)
        else:
            pass

        if config.Followers or config.Following:
            table = uTable(config.Followers)
            query = 'INSERT INTO {table} VALUES(%s,%s)'
            cursor.execute(query, (config.User_id, int(User.id)))

        conn.commit()
    except psycopg2.IntegrityError:
        pass

def tweets(conn, Tweet, config):
    try:
        time_ms = round(time.time()*1000)
        cursor = conn.cursor()
        # TODO: fix mentions and reply
        entry = (Tweet.id,
                    Tweet.id_str,
                    Tweet.tweet,
                    Tweet.lang,
                    Tweet.conversation_id,
                    Tweet.datetime,
                    Tweet.datestamp,
                    Tweet.timestamp,
                    Tweet.timezone,
                    Tweet.place,
                    Tweet.replies_count,
                    Tweet.likes_count,
                    Tweet.retweets_count,
                    Tweet.user_id,
                    Tweet.user_id_str,
                    Tweet.username,
                    Tweet.name,
                    Tweet.link,
                    json.dumps(Tweet.mentions),
                    Tweet.hashtags,
                    Tweet.cashtags,
                    Tweet.urls,
                    Tweet.photos,
                    Tweet.thumbnail,
                    Tweet.quote_url,
                    Tweet.video,
                    Tweet.geo,
                    Tweet.near,
                    Tweet.source,
                    time_ms,
                    Tweet.translate,
                    Tweet.trans_src,
                    Tweet.trans_dest,
                    config.PostgresAdditionalId,
                    "undefined",
                    "undefined",
                    "undefined",
                    "undefined",
                    "undefined",)
        cursor.execute('INSERT INTO tweets VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', entry)
        if config.Favorites:
            query = 'INSERT INTO favorites VALUES(?,?)'
            cursor.execute(query, (config.User_id, Tweet.id))
        if Tweet.retweet:
            query = 'INSERT INTO retweets VALUES(?,?,?,?,?)'
            _d = datetime.timestamp(datetime.strptime(Tweet.retweet_date, "%Y-%m-%d %H:%M:%S"))
            cursor.execute(query, (int(Tweet.user_rt_id), Tweet.user_rt, Tweet.id, int(Tweet.retweet_id), _d))
        
        if Tweet.reply_to:
            for reply in Tweet.reply_to:
                query = f"INSERT INTO replies VALUES('{Tweet.id}', {int(reply['id'])}, '{reply['screen_name']}')"
                cursor.execute(query)

        conn.commit()
    except Exception as e:
        print(f"ERROR: {str(e)}")
        pass
