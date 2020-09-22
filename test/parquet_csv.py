from pyspark.sql import SparkSession
import shutil,os
import operator
from pyspark.sql import functions as f

class parquetVsCsv:
    def __init__(self):
        self.spark=SparkSession.builder.getOrCreate()
        self.main_path=r'\Users\Cindy_Sun\testdata'

    #read original generated parquet,and change it into csv with header
    def parquet2Csv(self,entity):
        source_par=os.path.join(self.main_path,"source",entity)
        df=self.spark.read.parquet(source_par)
        df.show()
        df.printSchema
        csvpath=os.path.join(self.main_path,"csv",entity)
        if os.path.isdir(csvpath):
            self.delete_dir(csvpath)
        df.write.csv(csvpath,header=True)

    #After manually adjust the csv file, convert it into parquet
    def csv2parquet(self,entity):
        csvpath = os.path.join(self.main_path, "csv", entity)
        df=self.spark.read.csv(csvpath,header=True)
        df.show()
        target_par=os.path.join(self.main_path,"target",entity)
        if os.path.isdir(target_par):
            self.delete_dir(target_par)
        df.write.parquet(target_par)

    #check the dtypes discrepancy between original and new parquet, change the target dtype same with source
    def checkDtypePart(self,entity,pt_dt_col=None,ref_col=None):
        source_par=os.path.join(self.main_path,"source",entity)
        df=self.spark.read.parquet(source_par)
        df.show()
        target_par=os.path.join(self.main_path,"target",entity)
        df2=self.spark.read.parquet(target_par)
        df2.show()
        list_diff_df1=[]
        list_diff_df2=[]

        #change dtypes
        for i in range(0,len(df.dtypes)):
            if operator.ne(df.dtypes[i],df2.dtypes[i]):
                list_diff_df1.append(df.dtypes[i])
                list_diff_df2.append(df2.dtypes[i])
        dict1=dict(list_diff_df1)
        for key,value in dict1.items():
            df2=df2.withColumn(key,f.col(key).astype(value))
        df2.printSchema()
        df2.show()

        #with partition
        if pt_dt_col!=None:
            # df2=df2.withColumn("cn_pos_dt",df2["cn_pos_transaction_date"][1:10])
            if df2.columns.__contains__(pt_dt_col)==0:
                if ref_col!=None:
                    df2 = df2.withColumn(pt_dt_col, f.col(ref_col)[1:10])

            tmp_dir=os.path.join(self.main_path,"tmp",entity)
            if os.path.isdir(tmp_dir):
                self.delete_dir(tmp_dir)
            # df2.write.partitionBy("cn_pos_dt").parquet(tmp_dir)
            df2.write.partitionBy(pt_dt_col).parquet(tmp_dir)
        #without partition
        if pt_dt_col==None and ref_col==None:
            tmp_dir = os.path.join(self.main_path, "tmp", entity)
            if os.path.isdir(tmp_dir):
                self.delete_dir(tmp_dir)
            df2.write.parquet(tmp_dir)

        target_par = os.path.join(self.main_path, "target", entity)
        if os.path.isdir(target_par):
            self.delete_dir(target_par)
        shutil.move(tmp_dir, target_par)
        df2.printSchema()


    #delete existing entity folder
    def delete_dir(self,parent_path):
        dirlist=os.listdir(parent_path)
        for f in dirlist:
            filepath=parent_path+'\\'+f
            print(filepath)
            if os.path.isdir(filepath):
                try:
                    self.delete_dir(filepath)
                    os.rmdir(filepath)
                except Exception:
                    continue
            else:
                os.remove(filepath)
        os.rmdir(parent_path)


cv=parquetVsCsv()
cv.parquet2Csv("margin")
# cv.csv2parquet("sales_nike_direct_pos_detail")
# cv.checkDtypePart("sales_nike_direct_pos_detail","cn_pos_dt","cn_pos_transaction_date")
# cv.checkDtypePart("sales_nike_direct_pos_detail")

