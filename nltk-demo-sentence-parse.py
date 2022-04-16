from nltk.tokenize import sent_tokenize, word_tokenize

text = 'Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared.'

text_list = sent_tokenize(text)
for line in text_list:
    print(line)

text_ch = "撒发的 。水电费奥德赛苏打粉asd\n安师大发生地方啊水电费啊"
text_list_ch = sent_tokenize(text_ch)
for line in text_list_ch:
    print(line)
