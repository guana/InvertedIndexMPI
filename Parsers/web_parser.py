#!/usr/bin/python
'''
 Copyright 2011
 Written by Josh Davidson <joshuad AT ualberta DOT ca>
'''
import sys
import re

def usage():
    print("parser.py <filename>")

'''
Strips the data of HTML tags
'''

def htmlToText(data):        
    # remove the newlines
    data = data.replace("\n", " ")
    data = data.replace("\r", " ")
    data = data.replace("|", " ")
   
    # replace consecutive spaces into a single one
    data = " ".join(data.split())   
   
    # now remove the tags
    p = re.compile(r'<script[^<>]*?>.*?</script>|<style[^<>]*?>.*?</style>|<input[^<>]*?>|<[^<>]*?>|&.*?;|[.?!,":;\(\)\[\]\{\}\'\"#\*\\/<>]')
    data = p.sub('', data)
    
    data = " ".join(data.split())
   
    return data

def removeStopWords(data):
    stopwords = ["a" , "about" , "above" , "above" , "across" , "after" , "afterwards" , "again" , "against" , "all" , "almost" , "alone" , "along" , "already" , "also" , "although" , "always" , "am" , "among" , "amongst" , "amoungst" , "amount" , "an" , "and" , "another" , "any" , "anyhow" , "anyone" , "anything" , "anyway" , "anywhere" , "are" , "around" , "as" , "at" , "back" , "be" , "became" , "because" , "become" , "becomes" , "becoming" , "been" , "before" , "beforehand" , "behind" , "being" , "below" , "beside" , "besides" , "between" , "beyond" , "bill" , "both" , "bottom" , "but" , "by" , "call" , "can" , "cannot" , "cant" , "co" , "con" , "could" , "couldnt" , "cry" , "de" , "describe" , "detail" , "do" , "done" , "down" , "due" , "during" , "each" , "eg" , "eight" , "either" , "eleven" , "else" , "elsewhere" , "empty" , "enough" , "etc" , "even" , "ever" , "every" , "everyone" , "everything" , "everywhere" , "except" , "few" , "fifteen" , "fify" , "fill" , "find" , "fire" , "first" , "five" , "for" , "former" , "formerly" , "forty" , "found" , "four" , "from" , "front" , "full" , "further" , "get" , "give" , "go" , "had" , "has" , "hasnt" , "have" , "he" , "hence" , "her" , "here" , "hereafter" , "hereby" , "herein" , "hereupon" , "hers" , "herself" , "him" , "himself" , "his" , "how" , "however" , "hundred" , "ie" , "if" , "in" , "inc" , "indeed" , "interest" , "into" , "is" , "it" , "its" , "itself" , "keep" , "last" , "latter" , "latterly" , "least" , "less" , "ltd" , "made" , "many" , "may" , "me" , "meanwhile" , "might" , "mill" , "mine" , "more" , "moreover" , "most" , "mostly" , "move" , "much" , "must" , "my" , "myself" , "name" , "namely" , "neither" , "never" , "nevertheless" , "next" , "nine" , "no" , "nobody" , "none" , "noone" , "nor" , "not" , "nothing" , "now" , "nowhere" , "of" , "off" , "often" , "on" , "once" , "one" , "only" , "onto" , "or" , "other" , "others" , "otherwise" , "our" , "ours" , "ourselves" , "out" , "over" , "own," , "part" , "per" , "perhaps" , "please" , "put" , "rather" , "re" , "same" , "see" , "seem" , "seemed" , "seeming" , "seems" , "serious" , "several" , "she" , "should" , "show" , "side" , "since" , "sincere" , "six" , "sixty" , "so" , "some" , "somehow" , "someone" , "something" , "sometime" , "sometimes" , "somewhere" , "still" , "such" , "system" , "take" , "ten" , "than" , "that" , "the" , "their" , "them" , "themselves" , "then" , "thence" , "there" , "thereafter" , "thereby" , "therefore" , "therein" , "thereupon" , "these" , "they" , "thickv" , "thin" , "third" , "this" , "those" , "though" , "three" , "through" , "throughout" , "thru" , "thus" , "to" , "together" , "too" , "top" , "toward" , "towards" , "twelve" , "twenty" , "two" , "un" , "under" , "until" , "up" , "upon" , "us" , "very" , "via" , "was" , "we" , "well" , "were" , "what" , "whatever" , "when" , "whence" , "whenever" , "where" , "whereafter" , "whereas" , "whereby" , "wherein" , "whereupon" , "wherever" , "whether" , "which" , "while" , "whither" , "who" , "whoever" , "whole" , "whom" , "whose" , "why" , "will" , "with" , "within" , "without" , "would" , "yet" , "you" , "your" , "yours" , "yourself" , "yourselves" , "the"]
    filteredtext = [t for t in data.split() if t.lower() not in stopwords]
    filteredtext = " ".join(filteredtext)
    return filteredtext

def getHTMLPage(URL, length, f):
    if(URL != ""):
        line = f.readline()
        while(line != ""):
            if "Content-Length: " in line:
                length = line[15:]
                htmlPage = f.read(int(length)+1)
                return htmlPage
            else:
                line = f.readline()
    
'''
Parse out the data in the useable data for MapReduce
'''
def parseData(f):
    length = -1
    URL = ""
    line = f.readline()
    pages = dict()
    lookup_table = dict()
    while(line != ""):
        if "WARC-Target-URI: " in line:
            URL = line[16:]
        elif "Content-Length: " in line:
           length = line[15:]
           htmlPage = getHTMLPage(URL, int(length), f)
           textPage = htmlToText(str(htmlPage))
           filteredPage = removeStopWords(textPage)
           if not URL in pages:
                pages[URL] = filteredPage
        line = f.readline()
    i = 0
    filename = f.name+".parsed.full"
    fout = open(filename,"w")
    for page in pages:
        pagename = f.name+".parsed"+"."+repr(i)
        fout.write("###"+pagename+"\n")
        fout.write(pages[page]+"\r\n")
        lookup_table[page] = pagename
        i = i+1
    fout.close()
    filename = f.name+".parsed.lookup_table"
    fout = open(filename,"w")
    fout.write(repr(lookup_table))
    fout.close()
    
    
if __name__ == "__main__":
    if(len(sys.argv) < 2):
        usage()
    else:
        with open(sys.argv[1], "r") as f:
            parseData(f)
        f.close()
        
        
    
