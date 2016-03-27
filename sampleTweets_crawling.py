_author_ = "Vivek Ranjan"
import tweepy
import time

access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""
path="C:\Users\lenovo\Desktop\major2\data\\"

tweet_count = 0
file_name = ''

def make_fname():
    timevar = int(time.time() * 1000)
    filename = path + str(timevar) + '.lines'
    return filename

def my_on_data(data):
    global file_name, tweet_count
    if data.startswith('{') and 'status_id' in data:
        try:
            timevar = int(time.time() * 1000)
            f = open(file_name, 'a') #appending mode
            f.write(str(timevar) + '\t' + str(data) + '\n')
            f.close()
            tweet_count += 1
            if tweet_count % 100 == 0:
                print tweet_count
            if tweet_count % 500 == 0:
                file_name = make_fname()
        except:
            raise
    
            
def my_on_error(status_code):
    print 'Error: ', str(status_code)


def main():
    global tweet_count, file_name
    
    auth = tweepy.OAuthHandler(consumer_key,consumer_secret) 
    auth.set_access_token(access_token,access_token_secret)
    api = tweepy.API(auth)
    
    tweet_count = 0
    file_name = make_fname() 

    #streaming of data from twitter
    streamlistener = tweepy.StreamListener(api)
    streamlistener.on_data = my_on_data   #callback function
    streamlistener.on_error = my_on_error
    stream = tweepy.Stream(auth, listener=streamlistener, secure=True)
    stream.filter(languages=["en","es"],track=["a","the","i","to","and","is","in","it","you"])

    try:
	stream.sample()
    except:
	raise

if __name__ == '__main__':
    main()
