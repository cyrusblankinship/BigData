from os import sys
import csv

readfile = sys.argv[1]
writefile = sys.argv[2]


def csv_rows(filename):
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row2 in reader:
            yield(row2)


customer_dict = dict()
product_dict = dict()
customerID = 0

for row in csv_rows(readfile):
    product = row['Product ID']
    cost = float(row['Item Cost'])
    customer = row['Customer ID']
    if product not in product_dict.keys():
        product_dict[product] = cost
        customer_dict[product] = 1
        customerID = customer
    elif customerID != customer:
        customer_dict[product] += 1
        product_dict[product] += cost
        customerID = customer
    else:
        product_dict[product] += cost

with open(writefile, 'w', newline='') as csv_file:
    fieldnames = ['Product ID', 'Customer Count', 'Total Revenue']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for i in sorted(product_dict.keys()):
        writer.writerow({'Product ID': i,
                         'Customer Count': customer_dict[i],
                         'Total Revenue': product_dict[i]})
