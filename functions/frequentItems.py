
def allBaskets():
    dict1={}
    for b in range(1,101):
        dict1[b] = []
        for i in range(1,101):
            if b%i==0:
                dict1[b].append(i)
    for key,value in dict1.items():
        print ("购物篮编号："+str(key)+ "   购物篮"+str(key)+"中的项："+str(value))
    return(dict1)

def frequentItem():
    dict1=allBaskets()
    stat={}
    for m in range(1,101):
        count=0
        for n in range(1,len(dict1)+1):
            if m in dict1[n]:
               count=count+1
        if count>=5:
            stat[m]=count
    print("如果阈值是5：")
    for key,value in stat.items():
        print("频繁项： "+str(key)+" 频繁项出现次数："+str(value))
    return (stat)

def frequentPair():
    dict1=allBaskets()
    dict2=frequentItem()
    list2=list(dict2.keys())
    print("如果阈值为5，频繁项对如下")
    for i in range(len(list2)):
        for j in range(i+1,len(list2)-1):
            count=0
            # dict3 = {}
            for key,value in dict1.items():
                if list2[i] in list(value) and list2[j] in list(value):
                    count=count+1
                    # dict3[key]=value
            if count>=5:
                 # print("频繁项对："+ str(list2[i])+" , "+str(list2[j])+" ,频繁项对在同购物篮出现次数: "+str(count)+"购物篮为"+str(dict3))
                 print("频繁项对："+ str(list2[i])+" , "+str(list2[j])+" ,频繁项对在同购物篮出现次数: "+str(count))


def countAllitems():
    dict1=allBaskets()
    length=0
    for value in dict1.values():
        length=length+len(list(value))
    print("所有购物篮项数据之和为: "+str(length))

def maxBasketItems():
    dict1=allBaskets()
    length = 0
    for value in dict1.values():
        if length<=len(list(value)):
            length = len(list(value))
    print("最大的购物篮中有 " + str(length)+" 项")
    for key,value in dict1.items():
        if len(list(value))==length:
            print("最大的购物篮编号： "+ str(key)+" , 购物篮中的项有: "+ str(value))

frequentItem()