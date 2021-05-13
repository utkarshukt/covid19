from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import csv
from textblob import TextBlob

class authentication():
    def __init__(self):
        self.consumer_secret = 'aol1hPF2COBYcngSmmb4SxwPIcyaEbIiC5zsT0Te9Un9pYIpUG'
        self.consumer_key = 'dvrw2uCo6MKmw6IBhMAJK1NHm'
        self.access_token = "3123473020-bSYrdSYfAgvNca4njJhTcPQKubAhgHlgGMx4R31"
        self.access_token_secret = "1qt1zXApoR78P3V66aHnYTfSpK8fwVRva3lhgn2I87rNn"
        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
    def get_authentication(self):
        return self.auth

class twitter_streamer():
    def stream_tweet(self,filename,hashtaglist,num):
        listner = twitterlistener(filename,num)
        a = authentication()
        auth = a.get_authentication()
        stream = Stream(auth, listner)
        loc=[68.1766451354, 7.96553477623, 97.4025614766, 35.4940095078]
        stream.filter(track=['gocorna','coronago','covid19','coronavirus','coronainindia'],languages=['en'],locations=loc)


class twitterlistener(StreamListener):
    def __init__(self,filename,num):
        self.count=0
        self.num=num
        self.filename=filename
    def on_data(self,data):
        try:
            print(data)
            tweet = data.split('"text"')[1].split('"')[1]
            tweet_sentiment=TextBlob(tweet)
            sentiment=tweet_sentiment.sentiment.polarity
            created_date = data.split('"created_at"')[1].split('"')[1]
            print(self.count)

            d={'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
            date_time=created_date.split(' ')
            mm=str(d[date_time[1].lower()])
            dd=str(date_time[2])
            year=str(date_time[-1])
            final_date=year+"-"+mm+"-"+dd
            with open('test03-05(withsentiment).csv','a') as f:
                writer = csv.writer(f)
                writer.writerow([final_date, tweet,sentiment])

            self.count+=1

            if(self.count<self.num):
                return True
            else:
                f.close()
                return False

        except BaseException as e:
            print("error on data"+str(e))
        return True
    def on_error(self, status_code):
        if(status_code==420):
            return False
        print(status_code)


if __name__=="__main__":
    hashtaglist = ['covid19', 'covid19inindia', 'gocorna', 'corona', 'cornainindia']
    filename = 'twitterdata.csv'
    num = 50000
    twitter_streamer = twitter_streamer()
    twitter_streamer.stream_tweet(filename, hashtaglist, num)