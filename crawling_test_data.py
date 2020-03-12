# -*- conding: utf-8 -*-
# Created_at : 2020-03-11
# Python Crawling for tweet
# 참고페이지 :  https://jeongwookie.github.io/2019/06/10/190610-twitter-data-crawling/

from bs4 import BeautifulSoup
import GetOldTweets3 as got
import datetime
import time
import pandas as pd
import multiprocessing
#################################################################
value=('코로나','여성의날','여성','남성','침착맨','부산','AI','인공지능','딥러닝','SNS','혐오','엑소','아이유','한반도','헬조선','발전','원자력','기계학습','태연','집','보수','휴강','휴학','인터넷강의','세계사','살인','충동','book','corona','코스피','국민','문재인','탄핵','통진당','민주당','자유','서울','연기','개강','개학','아이돌','삼성','폭락','슈카','확진자','PC',' 감염','BTS','마스크','추경')
def crawling(name):
    #키워드 설정
    
    
    for index in range(1, len(value)):
        KEY_WORD = str(value[index])

        #날짜 설정(형식은 반드시 YYYY-MM-DD 형식으로 해주셔야합니다, 검색일이 7일이 넘어가면 안됩니다

        #저장할 파일위치 + 이름
        if name=='p1':
            START_DAY = "2020-03-09"
            END_DAY = "2020-03-10"
            print('p1')
            SAVE_PATH = "D:\\crawling_test_data\\test_data1.csv"
        elif name=='p2':
            START_DAY = "2020-03-07"
            END_DAY = "2020-03-08"
            print('p2')
            SAVE_PATH = "D:\\crawling_test_data\\test_data2.csv"
        elif name=='p3':
            START_DAY = "2020-03-05"
            END_DAY = "2020-03-06"
            print('p3')
            SAVE_PATH = "D:\\crawling_test_data\\test_data3.csv"
        elif name=='p4':
            START_DAY = "2020-03-03"
            END_DAY = "2020-03-04"
            print('p4')
            SAVE_PATH = "D:\\crawling_test_data\\test_data4.csv"
        elif name=='p5':
            START_DAY = "2020-03-01"
            END_DAY = "2020-03-02"
            print('p5')
            SAVE_PATH = "D:\\crawling_test_data\\test_data5.csv"
        elif name=='p6':
            START_DAY = "2020-02-28"
            END_DAY = "2020-02-29"
            print('p6')
            SAVE_PATH = "D:\\crawling_test_data\\test_data6.csv"
        elif name=='p7':
            START_DAY = "2020-02-26"
            END_DAY = "2020-02-27"
            print('p7')
            SAVE_PATH = "D:\\crawling_test_data\\test_data7.csv"
        elif name=='p8':
            START_DAY = "2020-02-24"
            END_DAY = "2020-02-25"
            print('p8')
            SAVE_PATH = "D:\\crawling_test_data\\test_data8.csv"
        elif name=='p9':
            START_DAY = "2020-02-22"
            END_DAY = "2020-02-23"
            print('p9')
            SAVE_PATH = "D:\\crawling_test_data\\test_data9.csv"
        else:
            START_DAY = "2020-02-18"
            END_DAY = "2020-02-19"
            print('p10')
            SAVE_PATH = "D:\\crawling_test_data\\test_data11.csv"
        
        #최대 검색할 트윗건수
        MAX = 5000


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
        for index in tweet:
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
        twitter_df.to_csv(SAVE_PATH,mode='a',header=False,index=False)

        print("파일 저장 완료")
        time.sleep(3600)

num_list = ['p1','p2','p3','p4','p5','p6','p7','p8','p9','p10']

if __name__ == '__main__':
    for i in range(100):
        pool = multiprocessing.Pool(processes=10)
        pool.map(crawling,num_list)
        pool.close()
        pool.join()
        
    
