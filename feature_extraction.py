_author_ = "vivek ranjan"
import os
import json
import re
import string
from stop_words import get_stop_words
from sklearn.feature_extraction.text import TfidfVectorizer

path = "C:\Users\lenovo\Desktop\eight_sem_major\\data\\"
#created a set of stop words of english and spanish text
en_stop_words = set(get_stop_words('en'))
es_stop_words = set(get_stop_words('spanish'))


hashtag_dict = {}

print len(en_stop_words)
print len(es_stop_words)

tfidf_vectorizer = TfidfVectorizer(min_df=1,sublinear_tf = True)

enTweet_ID_dict = {} #entweet pruned text id dictionary
esTweet_ID_dict = {} #estweet pruned text id dictionary

enID_hashtag_dict = {} #{ID: {hashtag1,2,...} or 'notag'}
esID_hashtag_dict = {} #{ID: {hashtag1,2,...} or 'notag'}


def preprocess_text(text) : 
    text=text.lower() #convert the text to lowercase
    text=re.sub('rt','',text)
    text=re.sub('((https?://[^\s]+)|(http?://[^\s]+))','',text)
    text=re.sub('@[^\s]+','',text)
    text=re.sub(r'#','',text)
    text=re.sub('\s+',' ',text).strip(' ')
    #remove punctuation
    text="".join([ch for ch in text if ch not in string.punctuation])
    return text
    
    


#accessing files in the path director
for filename in os.listdir(path) : 
    print "reading " + filename
    filepath=path+filename
    #open the file in reading mode
    fd=open(filepath,'r')
    
    for line in fd.read().rstrip().split("\n"):
        if line=="" :
            continue
        stream_data = line.split("\t")
        jsonObj = json.loads(stream_data[1])
        lang=jsonObj["lang"]
        hashtags=jsonObj["entities"]["hashtags"]
        id=str(jsonObj["id"])
        raw_text = jsonObj["text"]
        isRetweeted = jsonObj["retweeted"]
        
        if lang=="en" : 
            tmp_text = preprocess_text(raw_text) #removes hashtag url and username
            #remove stopwords            
            pruned_text = ' '.join([word for word in tmp_text.split() if word not in en_stop_words])
            #if pruned text is not empty
            if(pruned_text!='') : 
                # don't consider many ids have same text, overwrite previous ids sharing same text, save the last id
                enTweet_ID_dict[pruned_text]=id
                if hashtags == [] : 
                    enID_hashtag_dict[id] = "notag"
                if hashtags != [] : 
                    enID_hashtag_dict[id]=set()
                    for hashtagObj in hashtags : 
                        hashtag = hashtagObj["text"]
                        enID_hashtag_dict[id].add(hashtag)
                        if hashtag not in hashtag_dict :
                            hashtag_dict[hashtag]={}
                        if lang not in hashtag_dict[hashtag] : 
                            hashtag_dict[hashtag][lang]= set()
                        if not isRetweeted : 
                            hashtag_dict[hashtag][lang].add(id)

print "The number of hashtag in hastag_dict is " + str(len(hashtag_dict))