# coding:utf-8
import csv
import re
csv.field_size_limit(3000 * 1024 * 1024)
month_day = [31,29,31,30,31,30,31,31,30,31,30,31]
news_dict = {}
date_list = []

infile_1 = csv.reader(open('../data/nyt-comments-2020.csv','r'))
i = 0
for row in infile_1:
    cur_date = row[8].split(' ')[0]
    if news_dict.get(cur_date) == None:
        news_dict[cur_date] = [row[7]]
    else:
        news_dict[cur_date].append(row[7])

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

        date = '2020-' + str_month + '-' + str_day
        date_list.append(date)

outfile = open('../data/news.txt','w')

for date in date_list:
    news_dict[date] = list(set(news_dict[date]))
    for news in news_dict[date]:
        # news_rmsymbol = re.sub(r'((\’m)|(\'m)|(\'s)|(\’s)|(\’d)|(\'d)|(\’t)|(\'t)|(p\.m\.)|(a\.m\.)|[\…\!\@\#\$\%\^\&\*\(\)\-\_\+\=\{\}\[\]\:\;\"\'\<\,\>\.\?\/\`\~\”\“\‘\—\’\•]|[0-9])'," ",news)
        news_rmsymbol = re.sub(r'[^a-zA-Z ]'," ",news)

        tmp = news_rmsymbol.split()
        if len(tmp) < 20:
            continue
        news_rmspace = ' '.join(tmp)
        if len(news_rmspace) >= 20:
            outfile.write(date + '\t' + news_rmspace.lower() + '\n')


# print(news_dict['2020-02-29'])

