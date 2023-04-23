import twint
import nest_asyncio
from tqdm import tqdm


class Scrapper:
    def __init__(self):
        pass

    def profile(self, username):
        config = twint.Config()
        config.Username = username
        config.Hide_output = True
        config.Store_object = True

        nest_asyncio.apply()
        twint.run.Lookup(config)

        user = {}
        user["user_id"] = twint.output.users_list[-1].id
        user["username"] = twint.output.users_list[-1].username
        user["name"] = twint.output.users_list[-1].name
        user["is_private"] = twint.output.users_list[-1].is_private
        user["is_verified"] = twint.output.users_list[-1].is_verified
        user["join_date"] = twint.output.users_list[-1].join_date
        user["tweets_amount"] = twint.output.users_list[-1].tweets
        user["followers_amount"] = twint.output.users_list[-1].followers
        user["following_amount"] = twint.output.users_list[-1].following
        user["likes_amount"] = twint.output.users_list[-1].likes
        user["media_count"] = twint.output.users_list[-1].media_count

        return user

    def search(
        self,
        keywords,
        keywords_exact="",
        keywords_or="",
        keywords_stop="",
        hashtags="",
        min_replies=0,
        min_retweets=0,
        min_likes=0,
        since="",
        until="",
        limit=20,
    ):
        keywords_exact = f'"{keywords_exact}"' if keywords_exact != "" else "\b"
        keywords_stop = (
            " ".join(["-" + keyword for keyword in keywords_stop.split(" ")])
            if len(keywords_stop) > 0
            else "\b"
        )
        keywords_or = (
            "(" + " OR ".join(keywords_or.split(" ")) + ")"
            if len(keywords_or) > 0
            else "\b"
        )
        hashtags = (
            "(" + " OR ".join(hashtags.split(" ")) + ")" if len(hashtags) > 0 else "\b"
        )
        since = f"since:{since}" if since != "" else "\b"
        until = f"until:{until}" if until != "" else "\b"

        config = twint.Config()
        config.Search = f"{keywords} {keywords_exact} {keywords_or} {keywords_stop} {hashtags} min_replies:{min_replies} min_faves:{min_likes} min_retweets:{min_retweets} {since} {until} lang:id"
        config.Limit = limit
        config.Hide_output = True
        config.Store_object = True
        config.Account_email = ""
        config.Account_username = ""
        config.Account_password = ""

        res = []

        try:
            saved = len(twint.output.tweets_list)

            nest_asyncio.apply()
            twint.run.Search(config)

            result = twint.output.tweets_list[saved:]

            print("Get {0} Tweet".format(len(result)))
            it = 0

            for tweet in tqdm(result):
                it += 1
                # print(str(it) + " " + str(tweet.id))
                if not tweet.retweet:
                    user = self.profile(tweet.username)
                    res.append(
                        {
                            "datetime": tweet.datetime,
                            "id": tweet.id,
                            "user_id": tweet.user_id,
                            "username": tweet.username,
                            "is_verified": user["is_verified"],
                            "join_date": user["join_date"],
                            "followers": user["followers_amount"],
                            "following": user["following_amount"],
                            "likes": user["likes_amount"],
                            "media_count": user["media_count"],
                            "tweet": tweet.tweet,
                            "replies_count": tweet.replies_count,
                            "likes_count": tweet.likes_count,
                            "retweets_count": tweet.retweets_count,
                            "urls": tweet.urls,
                            "replies": [],
                        }
                    )
        except:
            print("=> error, skip to next tweet!")

        return res


scrapper = Scrapper()

print(scrapper.search('"formula e"'))
