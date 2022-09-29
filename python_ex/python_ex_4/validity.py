string = input()
for i in range(len(string)//2):
    string = string.replace('()','').replace('{}','').replace('[]','')
print(not len(string) > 0)