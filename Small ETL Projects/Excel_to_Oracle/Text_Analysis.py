givenstring="Lorem ipsum dolor! diam amet, consetetur Lorem magna. sed diam nonumy eirmod tempor. diam et labore? et diam magna. et diam amet."

class TextAnalyzer:
    
    def __init__(self,text):
        # remove punctuation
        formattedtext = text.replace(',','').replace('!','').replace('?','').replace('.','')

        #Assign to variable
        formattedtext = formattedtext.lower()
        self.fmttext = formattedtext
    
    def counting(self):

        word_list = []
        word_list = self.fmttext.split(' ')

        #to remove duplicates make string as set and made a dictionary to keep count of a word
        #used string count  function to get the cound in key value pair.
        freqMap = {}

        #if you want to have a specific word count
        word = "lorem"
        for i in set(word_list): 
            if i == word:
                freqMap[word] = word_list.count(word)
        return freqMap

    def printing(self):
        print("Formatted string is:",self.fmttext)
        print("Dict is:", self.counting())

text1 = TextAnalyzer(givenstring)
text1.printing()