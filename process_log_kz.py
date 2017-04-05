import pandas as pd
import numpy as np
import re
from pandas import Series, DataFrame
import codecs
import datetime

print('Please confirm the input data file is saved at /log_input as log.txt, and is coded with UTF-8\n')
bsdir = '../'
file = codecs.open(bsdir+'log_input/log.txt','r','utf-8')
df= DataFrame(columns=('address','time','item','signal','size'))
address,time,item,signal,size,timestruct= list(),list(),list(),list(),list(),list()
print('Reading data...\n')
cnt = 0  # counter for which line may be corrupted
for line in file:
    cnt +=1
    temp = line.split(' ')
    # using replace to get rid of excessive whitespace and quote
    address1 = temp[0].replace('\t','').replace(' ','')
    time1 = line[line.find("[")+1:line.find("]")].replace('\t','').replace(' ','').replace('-0400','')
    item1 = ''.join(re.findall('"([^"]*)"',line)).replace('\t','').replace(' ','').replace('\'','')
    signal1 = temp[-2].replace('\t','').replace(' ','').replace('\'','')
    size1 = temp[-1].replace('\n','').replace('\t','').replace(' ','')
    if (item1 == '') or (not (size1.isdigit() or (size1 == '-'))):
        print('size ')
        print('Line '+str(cnt)+':\t\"' + line + '\" is corrupted, thus cannot be read, skipping...\n')
        continue
    # if the pattern recognition is not realized for time structure, move to next line
    try:
        timestruct1 = datetime.datetime.strptime(time1.replace('\t','').replace(' ',''),'%d/%b/%Y:%H:%M:%S') # if time of visit can't be identified
        signal1 = int(signal1) # if HTTP code is not a number
    except ValueError:
        print('Line '+str(cnt)+':\t\"' + line + '\" is corrupted, thus cannot be read, skipping...\n')
        continue
    # for each line, appending to corresponding attributes if quality control is passed
    address.append(address1)
    time.append(time1)
    item.append(item1)
    signal.append(signal1)
    size.append(size1)
    timestruct.append(timestruct1)

# combine attributes to the full dataframe
df['address'] = address
df['time'] = time
df['item'] = item
df['signal'] = signal
df['size'] = size
df['timestruct'] = timestruct

# adding an extra attribute that records the time elapsed since the first visit request
start = df.timestruct[0]
time_delta = df['timestruct'].apply(lambda x:x-start)
df['sec_len'] = time_delta.apply(datetime.timedelta.total_seconds)

print('Reading finished!\n')

# save the cleaned dataframe if needs be
#df.to_csv(bsdir+'/log_input/log_clean.csv')


################################################
# 1. find the most frequent visitors' IPs
print('#########################################')
print('### Question 1: most frequent visits ####')
print('#########################################')
print('In process...')
df_goup_address = df.groupby('address').size()
df_goup_address = df_goup_address.sort_values(ascending = False)
print('Top 10 hosts is written to ~/log_output/hosts.csv\n')
Series.to_csv(df_goup_address[0:10],bsdir+'log_output/hosts.txt',header=True)


################################################
# 2. fine the most file that consumes the most bandwith
print('##############################################')
print('### Question 2: mostly requested resources ###')
print('##############################################')

print('Filtering out unauthorized requests and incorrect requests accessing nonexistent files\n')
df_suc = df[df.signal != 401]
df_suc = df_suc[['item','size']]
df_suc['size'] = df_suc['size'].apply(lambda x:x.replace('-','0'))  # failed file request will be assigned to a size of 0
df_suc['size'] = df_suc['size'].apply(lambda x:int(x))
print('Done!')
print('In process...')
df_group_sig = df_suc.groupby('item')
print('Calculating numbers of requests for each researce item...\n')
resource = df_group_sig.agg(sum)
resource = resource.sort_values('size',ascending=False)
print('Top 10 resources of bandwidth use is written to ~/log_output/resources.txt\n')
Series.to_csv(resource.index[0:10],bsdir+'log_output/resources.txt')


################################################
# 3. define supportive functions

# function that transfers seconds elapsed since the first visit (x) in to an index in the full dataframe (df0)
def time2index (df0,x):
    df1 = df0[df0['sec_len']>x]
    return df1.index[0]

# subsets what's between starting index (i) and (t) seconds away from line i in the original Dataframe (df0)
# input: Original datafram df0, index of starting location (i), time interval (t) seconds
# output: A subset of dataframe (df1)
def subset_time_forward (df0, i, t):
    df1 = df0[(df0['sec_len']>df0['sec_len'][i])&(df0['sec_len']<df0['sec_len'][i]+t)]
    return df1

# filters out the overlapping hours-to-report and return the n most frequent visits
# input:
#   time_visit0: dataframe sorted by 'n_visits':
#       'sec' starting seconds of all 1-hour time window with 1 minute increment and number of visits
#       'n_visits' number of visits in the corresponding time window
#   n: number of top frequencies wanted
#   t: overlap tolerance (1 hour in this case) in seconds
# output: 'sec' without overlapping, sorted by number of visits
def find_top(time_visit0,n,t):
    # use output_time and output_num to record the output sorted list
    output_time = list()
    output_num = list()
    # the first time window has the most visits, will remainin the output regardless of overlapping
    output_time.append(time_visit0['sec'][0])
    output_num.append(time_visit0['n_visit'][0])
    for i in range(0,len(time_visit0)):     # loop through all the time windows
        # output_time has all the current top frequently visited starting time of time windows
        # a new entries can be added only if it's starting time is far away enough from all current entries
        output_time_t = [abs(x - time_visit0['sec'][i]) for x in output_time] # find out the mininum distance between the new entry and all existent entries
        if min(output_time_t) > t: # if the new entry isn't overlapping with any current entries within t (1 hour)
            output_time.append(time_visit0['sec'][i]) # append the new entry to output
            output_num.append(time_visit0['n_visit'][i])
            if len(output_time) == n: # if we already have n entries in the output, terminate the loop
                output = DataFrame({'sec' : output_time,'n_visit': output_num})
                return output
    # loop finished, yet n entries were not found (due to testing data of short time span)
    print('Less than ' + str(n)+' time frames identified. Testing data is of short a time span\n')
    output = DataFrame({'sec': output_time, 'n_visit': output_num})
    return output

################################################
# main function
# find a primary list of all the starting seconds-after-elapse and the number of visits in the coming hour
print('####################################')
print('### Question 3: most busy hours ####')
print('####################################')

# define hour and minute in seconds
hour = 3600
minute = 60
last = df['sec_len'][len(df)-1] # last is the time elapsed in seconds for the whole file
x = 0   # x is the tick of every minute to start an one hour time window with
sec_list = list() # the output lists
n_visit = list()

print('Constructing time windows with steps of 1 minute and length of 1 hour...\n')
print('Calculating how many visits are within each time window...\n')
while x+hour < last: # end when the end of the time window reaches the end of the data's time span
    # we need to subset the dataframe with each time window and decide their sizes
    i = time2index(df,x)    # the time window subset starts at index i
    df1 = subset_time_forward(df,i,hour)    # df1 is the subset needed
    sec_list.append(x)  # mark down the starting time of current time window
    n_visit.append(len(df1))    # mark down the number of visits in the current time window
    x += minute # time window starts from the next minute

time_visit = DataFrame({'sec':sec_list,'n_visit':n_visit})
time_visit = time_visit.sort_values('n_visit',ascending=False)
# reindex time_visit according to the new order
time_visit = time_visit.reset_index(drop=True)
# resort the list, find the top 10 time points of visit without overlapping
print('Finding the 10 busiest hours that do not overlap with each other...\n')
busy_time_visit = find_top(time_visit,10,hour)

################################################
# transfering seconds elapsed to readable time
start = df.loc[0,'time']
start = datetime.datetime.strptime(start.replace('\t','').replace(' ',''),'%d/%b/%Y:%H:%M:%S%z')
busy_time_struct = list()
for i in range(len(busy_time_visit)):
    busy_time_struct.append(start+datetime.timedelta(seconds=float(busy_time_visit.loc[i,'sec'])))
busy_time = DataFrame({'Starting time of the hour': busy_time_struct,'Number of visits': busy_time_visit['n_visit']})
print('Top 10 resources of busy hours use is written to ~/log_output/hours.txt\n')
busy_time.to_csv(path_or_buf=bsdir+'log_output/hours.txt')



################################################
# 4. find the log-ins that is within time
print('####################################')
print('#### Feature 4: blocked log-ins ####')
print('####################################')
print('Blocked log-ins are those haappened within 5 minutes of 3 consecutive fails of logging in. \n')

df_fail = DataFrame.from_csv(bsdir+'df_fail.csv')
print('Please confirm: \n401 \n is the code for failed authorization\n')
print('Sunsetting the failed log-ins...\n')
df_fail = df[df['signal'] == 401]
#df_fail.to_csv(bsdir+'df_fail.csv')

print(df_fail)
block_ind = []  # index in the original dataframe that are blocked log-ins
# processing blocks for each host as several groups
grouped = df.groupby('address')
print('Finding the log-ins that need to be blocked for each of the groups...\n')
for name,group in grouped:   # each full group
    fail_group = group[group['signal'] == 401]  # a fail_group is all the failed files for on group
    i_fail = 2          # !!!!the third failed log-in can start to trigger the block !!!
    while i_fail < len(fail_group) and i_fail != 0:  # scan the failed log-ins until the end the the failed log-ins for the group
        diff = fail_group.sec_len.iloc[i_fail] - fail_group.sec_len.iloc[i_fail-2]
        index3 = [fail_group.index[i_fail], fail_group.index[i_fail - 1], fail_group.index[i_fail - 2]]
        print(index3)
        # there are 2 fails before this one within 20 seconds
        # need also to confirm the three fails are consecutive before triggering the block
        if diff < 20 and index3[2] == index3[1]-1 and index3[1] == index3[0]-1:
            print(index3)
            block = group[(group['sec_len'] > fail_group.sec_len.iloc[i_fail])&(group['sec_len'] < fail_group.sec_len.iloc[i_fail] + 300)]  # subset log requests in the coming 5 minutes after block is triggered
            block_ind.extend(block.index)
            time_btw_blocks = (fail_group['sec_len'] - fail_group.sec_len.iloc[i_fail])
            time_btw_blocks = time_btw_blocks.reset_index(drop=True)
            i_fail = np.argmax(time_btw_blocks > 319)   # !!!new i_fail equals the index in the failed logs that is 5 minutes and 20 seconds apart from the log triggered block. returning 0 if reaching the end of list !!!
        else:
            i_fail += 1  # if no block is triggered, continue and process the next failed log-in

df_blocked = df.loc[block_ind,['address','time','item','signal','size']]
print('The list of blocked log-in attempt is written to ~/log_output/hours.txt\n')
df_blocked.to_csv(path_or_buf = bsdir+'log_output/blocked.txt')
