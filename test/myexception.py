import logging
class MyException(Exception):

    def __init(self):
        pass

    def myexcept(self):
        sh=logging.StreamHandler()
        formater="%'acttime's-%'pathname's-%'levelname's-%'message's"
        logging.basicConfig(level=logging.INFO,format=formater,handlers=[sh])
        logging.error('bad execution, logout')
