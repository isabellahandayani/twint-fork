import datetime
import logging as logme


class user:
    type = "user"

    def __init__(self):
        pass

    def __str__(self) -> str:
        return f"""
        Username: {self.username}
        Name: {self.name}
        ID: {self.id}  
        Bio: {self.bio}  
        Location: {self.location}  
        URL: {self.url}  
        Join date: {self.join_date}  
        Tweets: {self.tweets}
        Following: {self.following}
        Followers: {self.followers}
        Likes: {self.likes}
        Media: {self.media_count}
        Private: {self.is_private}
        Verified: {self.is_verified}
        Avatar: {self.avatar}
        Listed count: {self.listed_count}
        """


User_formats = {"join_date": "%Y-%m-%d", "join_time": "%H:%M:%S %Z"}


# ur object must be a json from the endpoint https://api.twitter.com/graphql
def User(ur):
    logme.debug(__name__ + ":User")
    if "data" not in ur and "user" not in ur["data"]:
        msg = "malformed json! cannot be parsed to get user data"
        logme.fatal(msg)
        raise KeyError(msg)
    _usr = user()
    _usr.id = ur["data"]["user"]["rest_id"]
    _usr.name = ur["data"]["user"]["legacy"]["name"]
    _usr.username = ur["data"]["user"]["legacy"]["screen_name"]
    _usr.bio = ur["data"]["user"]["legacy"]["description"]
    _usr.location = ur["data"]["user"]["legacy"]["location"]
    _usr.url = "https://www.twitter.com/"
    # parsing date to user-friendly format
    _dt = ur["data"]["user"]["legacy"]["created_at"]
    _dt = datetime.datetime.strptime(_dt, "%a %b %d %H:%M:%S %z %Y")
    # date is of the format year,
    _usr.join_date = _dt.strftime(User_formats["join_date"])
    _usr.join_time = _dt.strftime(User_formats["join_time"])

    # :type `int`
    _usr.tweets = int(ur["data"]["user"]["legacy"]["statuses_count"])
    _usr.following = int(ur["data"]["user"]["legacy"]["friends_count"])
    _usr.followers = int(ur["data"]["user"]["legacy"]["followers_count"])
    _usr.likes = int(ur["data"]["user"]["legacy"]["favourites_count"])
    _usr.media_count = int(ur["data"]["user"]["legacy"]["media_count"])
    _usr.listed_count = int(ur["data"]["user"]["legacy"]["listed_count"])

    _usr.is_private = ur["data"]["user"]["legacy"]["protected"]
    _usr.is_verified = ur["data"]["user"]["legacy"]["verified"]
    _usr.avatar = ur["data"]["user"]["legacy"]["profile_image_url_https"]
    _usr.background_image = ""
    # TODO : future implementation
    # legacy_extended_profile is also available in some cases which can be used to get DOB of user
    return _usr
