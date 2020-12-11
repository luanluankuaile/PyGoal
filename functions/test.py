import logging
fs=logging.StreamHandler()
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
LOG_FORMAT= "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO,format=LOG_FORMAT,datefmt=DATE_FORMAT,handlers=[fs])
def test():
    # dict={"name":"Jim","age":18,"address":"No.18 Street"}
    # for key,value in dict.items():
    #     logging.info(key+" : "+str(value))
    dict1={}.fromkeys(["姓名","年龄","地址"],"test")
    del dict1["姓名"]
    dict2=dict(age="Jim")
    logging.debug(str(dict1))
    logging.info(dict2)

def test2(a):
    if int(a)<10:
        logging.info("a<10")
    elif int(a)>=10 and int(a)<20:
        logging.info("20>a>=10")
    else:
        logging.info("a>=20")

def test3(a,b):
    try:
        logging.info(a/b)
    except ZeroDivisionError:
        logging.error("除数为零")

def test4(a):
    while int(a)<10:
        logging.info(a)
        a=a+1
test()
test2(15)
test3(3,0)
test4(1)