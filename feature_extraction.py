_author_ = "vivek ranjan"
import os
import json
import re
import pickle
import string
from stop_words import get_stop_words
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import OrderedDict
from scipy import sparse,io
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

def removal_freqinfreq(tweet_ID_dict,freq_set,infrequency_set) :
    tweetid_ord_dict = OrderedDict()
    tweetid_dict_new = {}
    for key in tweet_ID_dict : 
        new_key = ' '.join([token for token in key.split() if token not in freq_set and token not in infrequency_set])
        if new_key != "" : 
            tweetid = tweet_ID_dict[key]
            tweetid_ord_dict[new_key] = tweetid
            tweetid_dict_new[tweetid] = new_key
    return tweetid_ord_dict, tweetid_dict_new

def save_in_pickle(obj,name) :
    with open('C:\Users\lenovo\Desktop\major2\\output\\'+name+'.pkl','wb') as f :
        pickle.dump(obj,f,pickle.HIGHEST_PROTOCOL)



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


en_tweetid_ord_dict , en_idtweet_dict = removal_freqinfreq(enTweet_ID_dict,enFrequency_set,enInFrequency_set)
es_tweetid_ord_dict , es_idtweet_dict = removal_freqinfreq(esTweet_ID_dict,esFrequency_set,esInFrequency_set)

print "After removing frequent and infrequent tokens, english tokens count : " + str(len(en_tweetid_ord_dict))
print "After removing frequent and infrequent tokens, spanish tokens count : " + str(len(es_tweetid_ord_dict))

###Mapping colId2tweettokens
def TweetID2rowID_map(tweetid_ordict) : 
    tweetIDrowID_dict = {}
    tweetid_list=tweetid_ordict.values()
    for index,tweetid in enumerate(tweetid_list) : 
        tweetIDrowID_dict[tweetid]= index
    return tweetIDrowID_dict
        
enTweetId_rowId_Dict = TweetID2rowID_map(en_tweetid_ord_dict)
esTweetId_rowId_Dict = TweetID2rowID_map(es_tweetid_ord_dict)
        
print "The number of tokens in enTweetidorddict" + str(len(enTweetId_rowId_Dict))
print "The number of tokens in esTweetidorddict" + str(len(esTweetId_rowId_Dict))


save_in_pickle(enTweetId_rowId_Dict, 'en_TweetID_rowID_dicts')
save_in_pickle(enTweetId_rowId_Dict, 'es_TweetID_rowID_dicts')
save_in_pickle(enID_hashtag_dict, 'enID2hashtags')
save_in_pickle(esID_hashtag_dict, 'esID2hashtags')

enTweet_ID_dict.clear()
esTweet_ID_dict.clear()
#count of entoken and es token after removing frequent and infrequent  format is token:occurence

enTokenCount_new_dict = {}
esTokenCount_new_dict = {}


token_count(en_tweetid_ord_dict,enTokenCount_new_dict) 
token_count(es_tweetid_ord_dict,esTokenCount_new_dict)

print "The number of english tokens after removing frequent and infrequent tokens is : " + str(len(enTokenCount_new_dict))
print "The number of spanish tokens after removing frequent and infrequent tokens is : " + str(len(esTokenCount_new_dict))

#constrcution of sparse matix for english tokens
#in sparse matrix
#the number of rows is the number of tweets
#the number of columnse is the features
en_tweet_sparse_mat = tfidf_vectorizer.fit_transform(en_tweetid_ord_dict.keys())
print "The  english sparse matrix dimension is : "
print en_tweet_sparse_mat.shape
es_tweet_sparse_mat = tfidf_vectorizer.fit_transform(es_tweetid_ord_dict.keys())
print "The spanish sparse matrix dimension is : "
print es_tweet_sparse_mat.shape

io.mmwrite("C:\Users\lenovo\Desktop\major2\\output\\en_tweet_sparseMatrix.mtx",en_tweet_sparse_mat)
io.mmwrite("C:\Users\lenovo\Desktop\major2\\output\\es_tweet_sparseMatrix.mtx",es_tweet_sparse_mat)

#all english tweet ids in the hashtag_dict that appear in new_entweetid_ordered dictionary

estweetid_set = set()



