_author_ = "vivek ranjan"
import os
import json
import re
import string
from stop_words import get_stop_words
from sklearn.feature_extraction.text import TfidfVectorizer

path = "C:\Users\lenovo\Desktop\major2\\data\\"
#created a set of stop words of english and spanish text
en_stop_words = set(get_stop_words('en'))
es_stop_words = set(get_stop_words('es'))

#ISO 639-1 code for spanish is es

hashtag_dict = {}

print len(en_stop_words)  #174 stop words in english
print len(es_stop_words)    #308 stop words in spanish

tfidf_vectorizer = TfidfVectorizer(min_df=1,sublinear_tf = True)

enTweet_ID_dict = {} #entweet pruned text id dictionary
esTweet_ID_dict = {} #estweet pruned text id dictionary

enID_hashtag_dict = {} #{ID: {hashtag1,2,...} or 'notag'}
esID_hashtag_dict = {} #{ID: {hashtag1,2,...} or 'notag'}

#in the form {token : occurence}
enToken_count_dict = {}
esToken_count_dict = {}

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
        
def token_count(text_list,count_dict) : 
    for text in text_list : 
        for token in re.split('\s',text) : 
            try : 
                count_dict[token] +=1
            except KeyError : 
                count_dict[token] = 1




#accessing files in the path director
for filename in os.listdir(path) : 
    print "reading " + filename
    filepath=path+filename
    #open the file in reading mode
    fd=open(filepath,'r')
    #for each single line in lines file without the whitespace in the right of line
    for line in fd.read().rstrip().split("\n"):
        
        if line=="" :
            continue

        stream_data = line.split("\t")
        #data from twitter is stored in json format in jsonOject file        
        jsonObj = json.loads(stream_data[1])
        lang=jsonObj["lang"]
        hashtags=jsonObj["entities"]["hashtags"]
        id=str(jsonObj["id"])
        raw_text = jsonObj["text"]
        isRetweeted = jsonObj["retweeted"]
        #processing of  english tweets
        if lang=="en" : 
            #print raw_text
            tmp_text = preprocess_text(raw_text) #removes hashtag url and username
            #print tmp_text            
            #remove stopwords            
            pruned_text = ' '.join([word for word in tmp_text.split() if word not in en_stop_words])
            #if pruned text is not empty
            #print pruned_text
            if(pruned_text!='') : 
                # don't consider many ids have same text, overwrite previous ids sharing same text, save the last id
                enTweet_ID_dict[pruned_text]=id
                #make a dictionary of hashtags present in a partcular id of tweet
                #if there is not hashtag in the tweet then just insert notag
                if hashtags == [] : 
                    enID_hashtag_dict[id] = "notag"
                #if there is any hashtag present in the tweet 
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
        if lang=="es" : 
            #print raw_text
            tmp_text = preprocess_text(raw_text) #removes hashtag url and username
            #print tmp_text            
            #remove stopwords            
            pruned_text = ' '.join([word for word in tmp_text.split() if word not in en_stop_words])
            #if pruned text is not empty
            #print pruned_text
            if(pruned_text!='') : 
                # don't consider many ids have same text, overwrite previous ids sharing same text, save the last id
                esTweet_ID_dict[pruned_text]=id
                #make a dictionary of hashtags present in a partcular id of tweet
                #if there is not hashtag in the tweet then just insert notag
                if hashtags == [] : 
                    esID_hashtag_dict[id] = "notag"
                #if there is any hashtag present in the tweet 
                if hashtags != [] : 
                    esID_hashtag_dict[id]=set()
                    for hashtagObj in hashtags : 
                        hashtag = hashtagObj["text"]
                        esID_hashtag_dict[id].add(hashtag)
                        if hashtag not in hashtag_dict :
                            hashtag_dict[hashtag]={}
                        if lang not in hashtag_dict[hashtag] : 
                            hashtag_dict[hashtag][lang]= set()
                        if not isRetweeted : 
                            hashtag_dict[hashtag][lang].add(id)
                            
print "Total number of hashtag in hashtag_dict is : " + str(len(hashtag_dict))
print "Total number of tweets in enlish is  : " + str(len(enTweet_ID_dict))

token_count(enTweet_ID_dict.keys(),enToken_count_dict)
token_count(esTweet_ID_dict.keys(),esToken_count_dict)

print "Number of English Tokens : " + str(len(enToken_count_dict))
print "Number of Spanish Tokens : " + str(len(esToken_count_dict))

##############################################################################
#############CALCULATING FREQUENCY OF TOKENS OF ENGLISH AND SPANISH###########
##############################################################################

enFrequency_set = set()
esFrequency_set = set()

#setting the threshold of frequency of english and spanish token

enFrequency_threshold = int(0.0008*len(enToken_count_dict))
esFrequency_threshold = int(0.004*len(esToken_count_dict))

for key in enToken_count_dict : 
    if enToken_count_dict[key]>=enFrequency_threshold:
        enFrequency_set.add(key)

for key in esToken_count_dict : 
    if esToken_count_dict[key]>=esFrequency_threshold : 
        esFrequency_set.add(key)
print "Number of English token (Frequent) : " + str(len(enFrequency_set))
print "Number of Spanish token (Frequent) : " + str(len(esFrequency_set))

enInFrequency_threshold = 7
esInFrequency_threshold = 5

enInFrequency_set=set()
esInFrequency_set = set()

for key in enToken_count_dict : 
    if enToken_count_dict[key]<=enInFrequency_threshold:
        enInFrequency_set.add(key)

for key in esToken_count_dict : 
    if esToken_count_dict[key]<=esInFrequency_threshold:
        esInFrequency_set.add(key)
        
print "Number of English token (InFrequent) : "+str(len(enInFrequency_set))
print "Number of Spanish token (InFrequent) : " + str(len(esInFrequency_set))

print "Filtered tweets in English (Frequent and infrequent) : " + str(len(enFrequency_set)+len(enInFrequency_set))
print "Filtered tweets in Spanish (Frequent and infrequent) : " + str(len(esFrequency_set)+len(esInFrequency_set))


