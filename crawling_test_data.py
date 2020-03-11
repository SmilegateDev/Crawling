# Created_at : 2020-03-11
# Python Crawling for tweet
# 참고페이지 :  https://jeongwookie.github.io/2019/06/10/190610-twitter-data-crawling/

from bs4 import BeautifulSoup
import GetOldTweets3 as got
import datetime
import time
import pandas as pd
from tqdm import tqdm_notebook

#################################################################

#키워드 설정
KEY_WORD = '코로나'

#날짜 설정(형식은 반드시 YYYY-MM-DD 형식으로 해주셔야합니다, 검색일이 7일이 넘어가면 안됩니다)
START_DAY = "2020-03-09"
END_DAY = "2020-03-10"


#저장할 파일위치 + 이름
SAVE_PATH = "C:\\Users\\Public\\test_data.csv"

#최대 검색할 트윗건수
MAX = 300


#################################################################

print("날짜 설정 시작")


days_range = []

#(검색 일수가 7일이 넘어가면 안됩니다)
start = datetime.datetime.strptime(START_DAY, "%Y-%m-%d") #시작 날짜 설정
end = datetime.datetime.strptime(END_DAY, "%Y-%m-%d") #끝나는 날짜 설정

date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

for date in date_generated:
    days_range.append(date.strftime("%Y-%m-%d"))


start_date = days_range[0]
end_date = (datetime.datetime.strptime(days_range[-1], "%Y-%m-%d") 
            + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

print("날짜 설정 완료")

# 키워드 설정
tweetCriteria = got.manager.TweetCriteria().setQuerySearch(KEY_WORD).setSince(start_date).setUntil(end_date).setMaxTweets(MAX)

tweet = got.manager.TweetManager.getTweets(tweetCriteria)

print("키워드 설정 완료")

tweet_list = []

print("크롤링 시작")
for index in tqdm_notebook(tweet):
    
    # 메타데이터 목록 
    username = index.username
    content = index.text


    # 결과 합치기
    info_list = [username, content]
    tweet_list.append(info_list)

print("크롤링 종료")


twitter_df = pd.DataFrame(tweet_list, 
                          columns = ["user_name", "text"])

# 컬럼 이름 변경
twitter_df = twitter_df.rename({'user_name' : 'title',
                                'text' : 'contents'}, axis= 'columns')

# csv 파일 만들기
twitter_df.to_csv(SAVE_PATH, index=False)

print("파일 저장 완료")