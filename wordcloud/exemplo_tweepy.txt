import tweepy
 
# API keyws that yous saved earlier
api_key = "..."
api_secrets = "..."
access_token = "..."
access_secret = "..."
 
# Authenticate to Twitter
auth = tweepy.OAuthHandler(api_key,api_secrets)
auth.set_access_token(access_token,access_secret)
 
api = tweepy.API(auth)
 
try:
    api.verify_credentials()
    print('Successful Authentication')
except:
    print('Failed authentication')