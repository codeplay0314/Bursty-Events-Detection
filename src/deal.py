# coding:utf-8
import csv
import re
csv.field_size_limit(1000 * 1024 * 1024)
month_day = [31,29,31,30,31,30,31,31,30,31,30,31]
news_dict = {}
date_list = []

infile_1 = csv.reader(open('./data/articles1.csv','r'))
for row in infile_1:
    if news_dict.get(row[5]) == None:
        news_dict[row[5]] = [row[9]]
    else:
        news_dict[row[5]].append(row[9])

infile_2 = csv.reader(open('./data/articles2.csv','r'))
for row in infile_2:
    if news_dict.get(row[5]) == None:
        news_dict[row[5]] = [row[9]]
    else:
        news_dict[row[5]].append(row[9])

infile_3 = csv.reader(open('./data/articles3.csv','r'))
for row in infile_3:
    if news_dict.get(row[5]) == None:
        news_dict[row[5]] = [row[9]]
    else:
        news_dict[row[5]].append(row[9])

for month in range(1,13):
    for day in range(1,month_day[month-1]+1):
        str_month = ''
        str_day = ''
        if month < 10:
            str_month = '0' + str(month)
        else:
            str_month = str(month)
        
        if day < 10:
            str_day = '0' + str(day)
        else:
            str_day = str(day)

        date = '2016-' + str_month + '-' + str_day
        date_list.append(date)

outfile = open('./data/news.txt','w')

for date in date_list:
    news_dict[date] = list(set(news_dict[date]))
    for news in news_dict[date]:
        news_rmsymbol = re.sub(r'((\’s)|(\’d)|(\’t)|(p\.m\.)|(a\.m\.)|[\…\!\@\#\$\%\^\&\*\(\)\-\_\+\=\{\}\[\]\:\;\"\'\<\,\>\.\?\/\`\~\”\“\‘\—\’\•]|[0-9])'," ",news)
        tmp = news_rmsymbol.split()
        if len(tmp) < 20:
            continue
        news_rmspace = ' '.join(tmp)
        if len(news_rmspace) >= 20:
            outfile.write(date + '\t' + news_rmspace + '\n')


# print(len(news_dict['2016-01-01']))

