import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,FloatType,IntegerType,DoubleType
import difflib
from difflib import SequenceMatcher
import sys
import re

class FuzzyTokenSortScore(object):
    
    def __init__(self,PY3= None,char_to_rem= None):
        self.PY3 = sys.version_info[0] == 3
        self.char_to_rem = str("").join([chr(i) for i in range(128, 256)])
        if self.PY3:
            self.translation_table = dict((ord(c), None) for c in self.char_to_rem)
            self.unicode = str
    
     
    def _process_and_sort(self,s, force_ascii, full_process=True):
        if full_process:
            if force_ascii:
                if type(s) is str:
                    if self.PY3:
                        s = s.translate(self.translation_table)
                    else:
                        s =  s.translate(None, self.char_to_rem)

            ts = re.sub('[^0-9a-zA-Z]+', ' ', s)
            ts = ts.lower()
            ts = ts.strip()
        else:
            ts = s
        
        tokens = ts.split()
        sorted_string = u" ".join(sorted(tokens))
        return sorted_string.strip()


    def token_sort_ratio(self,s1, s2, force_ascii=True, full_process=True):
        sorted1 = self._process_and_sort(s1, force_ascii, full_process=full_process)
        sorted2 = self._process_and_sort(s2, force_ascii, full_process=full_process)
        s = difflib.SequenceMatcher(None,sorted1,sorted2)
        return s.ratio()



udf_string_strip = udf(lambda x: x.strip() if pd.notnull(x) else None , StringType())

def append_name_comma(name):
    if len(name.split(","))>1:
        name1= name.split(",")[1]
        name2=name.split(",")[0]
        return name1+" "+name2
    else:
        return name

udf_append_name_comma = udf(lambda x:append_name_comma(x),StringType() )

def number_of_token(name):
    if name is not None:
        return len(name.split())
    else:
        return 0

udf_number_of_token = udf(lambda x:number_of_token(x),IntegerType() )
    
def number_of_matching_tokens(name1,name2):
    name1=name1.split()
    name2=name2.split()
    return len([w for w in name1 if w in name2])

udf_number_of_matching_tokens = udf(lambda x,y:number_of_matching_tokens(x,y),IntegerType() )

def ngram_score(name1,name2,n):
    name1_ngram= [name1[i:i+n] for i in range(len(name1)-1)]
    name2_ngram=[name2[i:i+n] for i in range(len(name2)-1)]
    return len([w for w in name1_ngram if w in name2_ngram])/float(max(len(name1_ngram),len(name2_ngram)))


udf_ngram_score1 = udf(lambda x,y:ngram_score(x,y,1) , FloatType())
udf_ngram_score2 = udf(lambda x,y:ngram_score(x,y,2) , FloatType())
udf_ngram_score3 = udf(lambda x,y:ngram_score(x,y,3) , FloatType())

def levenstein_dist(name1,name2):

    if name1 is not None and name2 is not None:
        return difflib.SequenceMatcher(None,name1,name2).ratio()
    else:
        return 0

udf_levenstein_dist = udf(lambda x,y:levenstein_dist(x,y),FloatType() )


def levenstein_token_sort(name1,name2):

    score_init = FuzzyTokenSortScore()
    score = score_init.token_sort_ratio(name1,name2)
    return score

udf_levenstein_token_sort = udf(lambda x,y:levenstein_token_sort(x,y),DoubleType()) 


def subset_match_ratio(d_cust,wc_cust):
    s= SequenceMatcher(None,d_cust,wc_cust)
    match = s.find_longest_match(0,len(d_cust),0,len(wc_cust))
    return match.size/float(max(len(d_cust),len(wc_cust)))

udf_subset_match_ratio = udf(lambda x,y:subset_match_ratio(x,y),FloatType() )

def alias_score_corp(d_cust,wc_alias):
    if pd.isnull(wc_alias):
        return None
    else:
        score =[]
        for i in wc_alias.split(";"):
            score.append(max(levenstein_dist(append_name_comma(i),d_cust),levenstein_token_sort(append_name_comma(i),d_cust)))
            
        return max(score)


def alias_score(d_cust,wc_alias):
    if pd.isnull(wc_alias):
        return None
    elif wc_alias == '':
        return None
    else:
        score =[]
        for i in wc_alias.split(";"):
            score.append(max(levenstein_dist(append_name_comma(i),d_cust),levenstein_token_sort(append_name_comma(i),d_cust)))

        return max(score)

udf_alias_score = udf(lambda x,y:alias_score(x,y),FloatType()  )
