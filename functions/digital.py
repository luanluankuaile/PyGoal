import glob
import logging
import time
import numpy as np
from myexception import MyException
import frequentItemsTwo


def fun():
   fs=logging.StreamHandler()
   date=time.strftime("%Y_%m_%d", time.localtime())
   fh=logging.FileHandler('C:/Users/Cindy/test_log_'+date+'.txt')
   outputformat="%(asctime)s-%(funcName)s-%(pathname)s-%(levelname)s:%(message)s"
   logging.basicConfig(level=logging.DEBUG,format=outputformat,handlers=[fs,fh])
   ar=np.array([[1,2,3],[4,5,6],[7,8,9],[10,11,12]])
   ar2=np.ones((4,3))
   ar4=np.asarray([5,4,'test',2])
   ar3=ar+ar2
   logging.info(ar)
   logging.info(ar.dtype)
   logging.info(ar2)
   logging.info(np.linspace(1,10,6).reshape((2,3)))
   logging.info(ar3)
   logging.info(ar==10)
   logging.info(ar4)
   logging.info(ar.ndim)
   logging.info(ar.shape)
   logging.info(ar.size)
   logging.info(ar4.ndim)
   logging.info(ar.nonzero())
   logging.info(ar.real)
   a=np.arange(10).reshape(2,5,1)
   logging.info(a)
   logging.info(a.ndim)
   list1=[12,25,5]
   it=iter(list1)
   x = np.fromiter(it,dtype=float)
   logging.info(x)
   st=glob.glob('*.py')
   logging.info(st)
   try:
       raise MyException
   except MyException as e:
       e.myexcept()
       logging.info("*************************")


fun()
#frequentItemsTwo.allBaskets()
frequentItemsTwo.allBaskets()
