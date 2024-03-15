import os
import pandas as pd
from multiprocessing.connection import Client
import tweepy
import time
import apiData
import time

def search_tweet(client):
    tweets = client.search_all_tweets(query = keyword, max_results=10, start_time = set_start_time, end_time = set_end_time,
                                    expansions=apiData.expansions, 
                                    tweet_fields=apiData.tweet_fields, 
                                    user_fields=apiData.user_fields, 
                                    media_fields = apiData.media_fields)
    return tweets
    
def search_tweet_next_token(client, token):
    tweets = client.search_all_tweets(query = keyword, max_results=100, start_time = set_start_time, end_time = set_end_time, next_token = token,
                                    expansions=apiData.expansions, 
                                    tweet_fields=apiData.tweet_fields, 
                                    user_fields=apiData.user_fields, 
                                    media_fields = apiData.media_fields)
    return tweets

def user_data_parser(tweet_data):
    for user_data in includes_users_data:
        if tweet_data.author_id == user_data['id']:
            row['AccountID'] = [user_data['id']]
            row['AccountNickname'] = user_data['name']
            row['AccountName'] = user_data['username']
            row['AccountCreated'] = user_data['created_at']
            row['AccountDescription'] = user_data['description']
            row['AccountEntities'] = user_data['entities']
            row['AccountDescriptionHashtag'] = user_entities_hashtags_parser(user_data['entities'])
            row['AccountDescriptionURL'] = user_entities_links_parser(user_data['entities'])
            row['AccountDescriptionMention'] = user_entities_mentions_parser(user_data['entities'])
            row['AccountLocation'] = user_data['location']
            row['AccountProfileImageURL'] = user_data['profile_image_url']
            row['AccountPublicMetrics'] = user_data['public_metrics']
            row['AccountFollowersCount'] = user_data.public_metrics['followers_count']
            row['AccountFollowingCount'] = user_data.public_metrics['following_count']
            row['AccountTweetCount'] = user_data.public_metrics['tweet_count']
            row['AccountListedCount'] = user_data.public_metrics['listed_count']
            row['AccountLikeCount'] = user_data.public_metrics['like_count']
            row['AccountURL'] = user_data['url']
            row['AccountVerified'] = user_data['verified']

def tweet_data_parser(tweet_data):
    row['TweetID'] = [tweet_data['id']]
    row['TweetContent'] = tweet_data['text']
    row['AuthorID'] = [tweet_data['author_id']]
    row['OriginalTweetID'] = [tweet_data['conversation_id']]
    row['TweetCreated'] = tweet_data['created_at']
    row['TweetEntities'] = tweet_data['entities']
    row['TweetContentHashtag'] = tweet_entities_hashtags_parser(tweet_data['entities'])
    row['TweetContentURL'] = tweet_entities_links_parser(tweet_data['entities'])
    row['TweetMention'] = tweet_entities_mentions_parser(tweet_data['entities'])
    row['TweetMedia'], row['TweetMediaURL'] = tweet_entities_media_parser(tweet_data['entities'])
    row['OriginalTweetAuthorID'] = [tweet_data['in_reply_to_user_id']]
    row['TweetLanguage'] = tweet_data['lang']
    row['TweetPublicMetrics'] = tweet_data['public_metrics']
    row['TweetRetweetCount'] = tweet_data.public_metrics['retweet_count']
    row['TweetReplyCount'] = tweet_data.public_metrics['reply_count']
    row['TweetLikeCount'] = tweet_data.public_metrics['like_count']
    row['TweetQuoteCount'] = tweet_data.public_metrics['quote_count']
    row['TweetBookmarkCount'] = tweet_data.public_metrics['bookmark_count']
    row['TweetImpressionCount'] = tweet_data.public_metrics['impression_count']
    row['TweetReferencedTweets'] = tweet_data['referenced_tweets']
    row['TweetSource'] = tweet_data['source']
    row['TweetPlace'] = tweet_data['geo']
    row['TweetContextAnnotations'] = tweet_data['context_annotations']

def user_entities_hashtags_parser(entities):
    hashtags = []
    try:
        if entities is not None and 'description' in entities:
            if entities['description'] is not None and 'hashtags' in entities['description']:
                for hashtag in entities['description']['hashtags']:
                    hashtags.append(hashtag['tag'])

        hashtags = list(set(hashtags))

        if not hashtags:
            hashtags = None

        return hashtags

    except:
        temp_e = "계정_해시태그_에러"
        return temp_e

def user_entities_mentions_parser(entities):
    mentions = []
    try:
        if entities is not None and 'description' in entities:
            if entities['description'] is not None and 'mentions' in entities['description']:
                for mention in entities['description']['mentions']:
                    mentions.append(mention['username'])

        mentions = list(set(mentions))

        if not mentions:
            mentions = None

        return mentions

    except:
        temp_e = "계정_멘션_에러"
        return temp_e
    
def user_entities_links_parser(entities):
    links = []
    try:
        #소개글에 포함된 URL
        if entities is not None and 'url' in entities:
            if entities['url'] is not None and 'urls' in entities['url']:
                for link in entities['url']['urls']:
                    if link['expanded_url'].startswith(('http://', 'https://')) and not any(keyword in link['expanded_url'] for keyword in ['twitter']):
                        links.append(link['expanded_url'])

        #계정_프로필_URL
        if entities is not None and 'description' in entities:
            if entities['description'] is not None and 'urls' in entities['description']:
                for link in entities['description']['urls']:
                    if link['expanded_url'].startswith(('http://', 'https://')) and not any(keyword in link['expanded_url'] for keyword in ['twitter']):\
                        links.append(link['expanded_url'])
        
        links = list(set(links))

        if not links:
          links = None

        return links
    
    except:
        temp_e = '계정_링크_에러'
        return temp_e

def tweet_entities_hashtags_parser(entities):
    hashtags = []
    try:
        if entities is not None and 'hashtags' in entities:
            for hashtag in entities['hashtags']:
                hashtags.append(hashtag['tag'])

        hashtags = list(set(hashtags))

        if not hashtags:
            hashtags = ""

        return hashtags

    except:
        temp_e = "트윗_해시태그_에러"
        return temp_e
    
def tweet_entities_mentions_parser(entities):
    mentions = []
    try:
        if entities is not None and 'mentions' in entities:
            for mention in entities['mentions']:
                mentions.append(mention['username'])

        mentions = list(set(mentions))

        if not mentions:
            mentions = None

        return mentions

    except:
        temp_e = "트윗_멘션_에러"
        return temp_e

def tweet_entities_links_parser(entities):
    links = []
    try:
        if entities is not None and 'urls' in entities:
            for link in entities['urls']:
                if link['expanded_url'].startswith(('http://', 'https://')) and not any(keyword in link['expanded_url'] for keyword in ['twitter']):
                    links.append(link['expanded_url'])

        links = list(set(links))

        if not links:
            links = None

        return links
    
    except:
        temp_e = '트윗_링크_에러'
        return temp_e

def tweet_entities_media_parser(entities):
    medias = []
    medias_url = []
    try:
        if includes_media_data is not None and entities is not None and 'urls' in entities:
            for media in entities['urls']:
                if 'media_key' in media:
                    for media_data in includes_media_data:
                        if media['media_key'] == media_data['media_key']:
                            row_midea = {}
                            row_midea['미디어_식별ID'] = media_data['media_key']
                            row_midea['미디어_타입'] = media_data['type']
                            row_midea['미디어_URL'] = media_data['url']
                            row_midea['미디어_길이'] = media_data['duration_ms']
                            row_midea['미디어_미리보기_URL'] = media_data['preview_image_url']
                            row_midea['미디어_참여지표'] = media_data['public_metrics']
                            medias.append(row_midea)
                            if media_data['type'] == 'photo':
                                medias_url.append(media_data['url'])
                            elif media_data['type'] == 'video':
                                medias_url.append(media_data['preview_image_url'])
                                
        medias_url = list(set(medias_url))

        if not medias:
            medias = None
            medias_url = None
        return medias, medias_url
    except:
        temp_e = '미디어_에러'
        return temp_e, temp_e
    
def print_data():
    count = 1
    for key, value in row.items():
        print("{}. {} : {}".format(count, key, value))
        count += 1

def include_tweet_data_parser():
    if includes_tweets_data is not None:
            for includes_tweet in includes_tweets_data:
                tweet_id = includes_tweet['id']
                tweet_text = includes_tweet['text']
                tweet_context_annotations = includes_tweet['context_annotations']
                tweet_author_id = includes_tweet['author_id']
                tweet_conversation_id = includes_tweet['conversation_id']
                tweet_created_at = includes_tweet['created_at']
                tweet_entities = includes_tweet['entities']
                tweet_hashtag = tweet_entities_hashtags_parser(tweet_data['entities'])
                tweet_url = tweet_entities_links_parser(tweet_data['entities'])
                tweet_media = tweet_entities_media_parser(tweet_data['entities'])
                tweet_in_reply_to_user_id = includes_tweet['in_reply_to_user_id']
                tweet_lang = includes_tweet['lang']
                tweet_public_metrics = includes_tweet['public_metrics']
                tweet_referenced_tweets = includes_tweet['referenced_tweets']
                tweet_source = includes_tweet['source']
                tweet_geo = includes_tweet['geo']

def save_csv():
    if os.path.isfile(csv_filename):
        df.to_csv(csv_filename, mode='a', header=False, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")



client = tweepy.Client(apiData.Bearer_token)
keyword = '#'                       #검색 키워드 입력
set_start_time = '2023-10-15T00:00:01Z'     #기간설정
set_end_time = '2024-01-15T23:59:59Z'       #기간설정
csv_filename = 'search_{}.csv'.format(keyword)

row = {}
data_list = []
cycle_count = 1
total_count = 0
next_token = None

while cycle_count:
    tweets = search_tweet_next_token(client, next_token)
    if 'next_token' in tweets.meta:
        next_token = tweets.meta['next_token']
        total_count += tweets.meta['result_count']
        #print("tweet : ", tweets)
        print('next_token : ', next_token)
        print('-------| cycle_count : {} |-------| result_count : {} |-------| total : {} |-------'.format(cycle_count, tweets.meta['result_count'], total_count))
        cycle_count += 1

    else:
        total_count += tweets.meta['result_count']
        print('-------| cycle_count : {} |-------| result_count : {} |-------| total : {} |-------'.format(cycle_count, tweets.meta['result_count'], total_count))
        cycle_count = None
        print('--------------------| End |--------------------')

    if tweets.data is not None:
        df = pd.DataFrame()
        tweets_data = tweets.data

        includes_users_data = tweets.includes['users']

        if 'media' in  tweets.includes:
            includes_media_data = tweets.includes['media']
        else :
            includes_media_data = None
        if 'tweets' in tweets.includes:
            includes_tweets_data = tweets.includes['tweets']
        else :
            includes_tweets_data = None

        for tweet_data in tweets_data:
            row = {}
            tweet_data_parser(tweet_data)
            user_data_parser(tweet_data)
            row['Include'] = None
            df = df._append(row, ignore_index=True)
            #print_data()

        if includes_tweets_data is not None:
            for include_tweet_data in includes_tweets_data:
                row = {}
                tweet_data_parser(include_tweet_data)
                user_data_parser(include_tweet_data)
                row['Include'] = 'True'
                df = df._append(row, ignore_index=True)
                #print_data()
        save_csv()

    time.sleep(3.5)