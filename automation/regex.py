import re
# ph_num = re.compile(r'(\(\d{3}\))-(\d{3}-\d{4})')
# mo = ph_num.search('My number is (415)-555-4242.')
# print(mo.groups())

pattern = re.compile(r'Cat(erpillar|strophe|s|egory)')
match = pattern.search('The Cats are what snehitha likes.')
print(match.group())