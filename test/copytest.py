import copy

def testcopy():
    # listo=[1,2,3,[1,2,3]]
    # list1=copy.copy(listo)
    # list2=copy.deepcopy(listo)
    # list3=listo[:]
    # list4=list(listo)
    # list5=listo
    # listo[3]=[4,5,6]
    # print(str(listo)+','+str(id(listo)))
    # print(str(list1)+','+str(id(list1)))
    # print(str(list2)+','+str(id(list2)))
    # print(str(list3)+','+str(id(list3)))
    # print(str(list4)+','+str(id(list4)))
    # print(str(list5)+','+str(id(list5)))
    listo=(1,2,3,4)
    list1=copy.copy(listo)
    list2=copy.deepcopy(listo)
    # list3=listo[:]
    # list4=list(listo)
    list5=listo
    listo=(2,3,4,5)
    # listo[3]=[4,5,6]
    print(str(listo)+','+str(id(listo)))
    print(str(list1)+','+str(id(list1)))
    print(str(list2)+','+str(id(list2)))
    # print(str(list3)+','+str(id(list3)))
    # print(str(list4)+','+str(id(list4)))
    print(str(list5)+','+str(id(list5)))

testcopy()