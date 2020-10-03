# from commons.metadata.data_quality_check_log import DataQualityCheckLog
# from commons,metadata.run_info import get_batch_id,get_date

from pyspark.sql.functions import coalesce, count, lit, col
from pyspark.sql import Row

class DataQualityCheck:
    def __init__(self,run_date=None,data_qulity_check_log_path=None,validators={},data_type="",):
        if validators is None:
            validators=[]
        # self._data_qulity_check_log_path=data_qulity_check_log_path
        self._data_type=data_type
        self._validators=validators
        self.run_date=run_date



    def data_clean(self,df,spark):
        original_columns=df.columns
        total_rows=df.count()
        if not self._validators:
            invalid_df=spark.createDataFrame([Row(rule_id='0',rule_description='0')])
            invalid_df=invalid_df.filter(invalid_df.rule_id!='0')
            matadata_df=self.prepare_dq_check_df(invalid_df,total_rows)
            # self.log_metadata(matadata_df)
            return df, invalid_df

        validation_msg_condition=""
        validation_rule_condition=""
        derive_column_names=[]
        for index,validator in enumerate(self._validators,start=1):
            derive_column_names.append(validator.derive_validation_column)
            df=validator.run_check(df)
            validation_msg_condition+="when {col}='1' then '{rule_desc}'".format(col=validator.derive_validation_column,
                                                                                 rule_desc=validator.description)
            validation_rule_condition+="when {col}='1' then '{rule_id}'".format(col=validator.derive_validation_column,
                                                                                 rule_id=validator.index)

        validation_msg_condition="case {0} end as rule_description".format(validation_msg_condition)
        validation_rule_condition="case {0} end as rule_id".format(validation_rule_condition)
        derive_cols=[col(name) for name in derive_column_names]
        df=df.withColumn("invalid",coalesce(*derive_cols))
        df_clean=df.filter(df.invalid.isNull()).select(original_columns)
        invalid_df=df.filter(df.invalid=="1")
        invalid_df.createOrReplaceTempView("rulechecktable")
        qry="select *,{message},{rule_id} from rulechecktable".format(message=validation_msg_condition,rule_id=validation_rule_condition)
        invalid_df=spark.sql(qry)
        invalid_df=invalid_df.drop(*derive_column_names)

        metadata_df=self.prepare_dq_check_df(invalid_df,total_rows)
        # self.log_metadata(metadata_df)

        return df_clean,invalid_df




    # def log_metadata(self,metadata_df):
    #     metadata_df.show()
    #     if self._data_qulity_check_log_path is not None:
    #         qt_log=DataQualityCheckLog(self._data_qulity_check_log_path)
    #         qt_log.write_quality_check_log(metadata_df)


    def prepare_dq_check_df(self,invalid_df,total_rows):
        # check_date=get_date(self.run_date)
        # invalid_df=invalid_df.withColumn("batch_id",lit(get_batch_id(self.run_date)))
        invalid_df=invalid_df.groupBy(["batch_id","rule_id","rule_description"]).agg(count(lit(1)).alias("invalid_rows"))
        invalid_df=invalid_df.withColumn("total_rows",lit(total_rows))\
            .withColumn("data_type",lit(self._data_type))\
            # .withColumn("check_date",lit(check_date))
        return invalid_df.select(
            ["batch_id","data_type","rule_id","rule_description","total_rows","invalid_rows","check_date"]
        )
