# Jaccard Distance
ALPHA = 0.7

def pseudo_jaccard(a, b):
    return 1 - max(float(len(a & b))/len(a), float(len(a & b))/len(b))

def jaccard(a, b):
    return 1 - float(len(a & b))/len(a | b)

def process_traditional():
    event_dict = {}
    cur_date = ""
    event_list = []
    for line in open("result_ver1.0.txt"):
        date = line[17: 27]
        if cur_date == "":
            cur_date = date
        elif cur_date != date:
            event_dict[cur_date] = event_list
            event_list = []
            cur_date = date
        event = set(line[30: len(line) - 2].split(' '))
        event_list.append(event)
    return event_dict

def process_innovative():
    prob_dict = {}
    event_dict = {}
    cur_date = ""
    event_list = []
    for line in open("result_ver2.0.log"):
        line = line.strip("\n")
        if line[0] != '[':
            date = line
            if cur_date == "":
                cur_date = date
            elif cur_date != date:
                event_dict[cur_date] = event_list
                event_list = []
                cur_date = date
            continue
        event_line = line.split('|')[0]
        prob = line.split('|')[1]
        event = set(event_line[1: len(event_line) - 1].split(' '))
        event_list.append(event)
        prob_dict[str(event)] = prob
    return event_dict, prob_dict

def min_val_count(dict1, dict2):
    val_count = {}
    set_1 = set(dict1.keys())
    set_2 = set(dict2.keys())
    key_set = set.union(set_1, set_2)
    for key in key_set:
        if key in dict1:
            if key in dict2:
                val_count[key] = max(len(dict1[key]), len(dict2[key]))
            else:
                val_count[key] = len(dict1[key])
        else:
            val_count[key] = len(dict2[key])
    return val_count


if __name__ == '__main__':
    traditional_bursty_event = process_traditional()
    innovative_bursty_event, event_prob = process_innovative()
    date_event_count = min_val_count(traditional_bursty_event, innovative_bursty_event)
    # print(date_event_count)
    for date in date_event_count:
        print(date, date_event_count[date])
    # for date in traditional_bursty_event:
    #     cnt = 0
    #     if(date in innovative_bursty_event):
    #         event_list_1 = traditional_bursty_event[date]
    #         event_list_2 = innovative_bursty_event[date]
    #         for event_1 in event_list_1:
    #             for event_2 in event_list_2:
    #                 distance = jaccard(event_1, event_2)
    #                 if distance < ALPHA:
    #                     # print(distance, event_1, event_2, event_prob[str(event_2)])
    #                     cnt += 1
    #                     break
    #     print(date, cnt)