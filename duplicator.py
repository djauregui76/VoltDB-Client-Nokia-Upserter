import csv
dataList=[]
dataList1=[]
reader = csv.reader(open('c:\\python27\\s6a.txt', 'rb'))
#writer = csv.writer(open('c:\\python27\\s6acomplete.txt', 'ab'))

reader1 = csv.reader(open('c:\\python27\\s1u.txt', 'rb'))
#writer1 = csv.writer(open('c:\\python27\\s1ucomplete.txt', 'ab'))

for row in reader:
    dataList.append(row)
for x in range(0, 500):
    writer = csv.writer(open('c:\\python27\\s6a' + str(x) + 'complete.txt', 'ab'))
    for y in range(0,2):
        for data_row in dataList:
            writer.writerow(data_row)

for row1 in reader1:
    dataList1.append(row1)
for x in range(0, 500):
    writer1 = csv.writer(open('c:\\python27\\s1u' + str(x) + 'complete.txt', 'ab'))
    for y in range(0,2):
        for data_row1 in dataList1:
            writer1.writerow(data_row2)
