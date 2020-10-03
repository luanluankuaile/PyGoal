from data_check_process import *
from valid_rules import *
from pyspark.sql.functions import trim,lit
# from commons.config.config_util import get_param,get_param_default
# from commons.etl.decorator import execute_sql, decorate_functions
# from commons.etl.etl import ETL
# from commons.etl.df_util import add_columns
# from commons.time_utils import convert_datetimes_in_dataframe, convert_dates_in_dataframe
# from commons.email.email_operator import send_email


stage="raw_to_cln"


def fulfill_template(etl,template,default_layer=None):
    variables=dict(domain=etl.domain,
                   lz_layer=etl._lz_layer,
                   raw_layer=etl._raw_layer,
                   cleansed_layer=etl._cleansed_layer,
                   harmonized_layer=etl._harmonized_layer,
                   entity=etl._entiry,
                   ds=etl._run_date,
                   cn_pt_dt=etl._cn_pt_dt)
    if etl._suffix:
        variables["suffix"]="_"+etl._suffix
    else:
        variables["suffix"]=""
    if default_layer:
        variables["layer"]=default_layer
    variables.update(etl.buckets)
    return template.format(**variables)


# @decorate_functions(execute_sql,stage)
class ETLFromRawToCleansed(ETL):
    def __init__(self,config):
        super(ETLFromRawToCleansed,self)
        self.action_layer=self._cleansed_layer
        self._validators={}
        if "validators" in config[stage]:
            self._validators=get_param(config,stage,"validators")
            self._quarantine_path=get_param(config,stage,"quarantine","path")

    def extract(self,dfs):
        super(ETLFromRawToCleansed,self).extract(dfs)
        self._source_dir_t=self.fullfill_template(self,self._source_dir_t,default_layer=self._raw_layer)
        dfs["raw"]=self.spark.read.load(path=self._source_dir_t,format="parquet").withColumn(self._run_dt_col,lit(self.run_date))
        return dfs


    def transform(self,dfs):
        self._register_dfs(dfs)
        validators=[]
        if "null_value_validator_columns" in self._validators:
            validators.append(NullValueValidator(columns=self._validators["null_value_validator_columns"],
                                                 description="[{0}] must not be null".format("".join(str(e+"") for e in self._validators["null_value_validator_columns"])),
                                                 derive_validation_column="nullcheck"))

        if "duplicate_row_validator_columns" in self._validators:
            validators.append(DuplicateRowValidator(columns=self._validators["duplicate_row_validator_columns"],
                                                    description="[{0}] must be unique".format("".join(str(e+"") for e in self._validators["null_value_validator_columns"])
                                                                                              .replace("'","''")),
                                                    derive_validation_column="duplicatecheck"))

        if "nonstrict_duplicate_validator" in self._validators:
            validators.append(NonstrictDuplicateValidator(nonstrict_duplicate_validators_config=self._validators["nonstrict_duplicate_validator"],
                                                          description="[{0}] must be unique (nonstrict)".format("".join(str(e+"") for e in self._validators["nonstrict_duplicate_validator"]["columns"]).replace("'","''")),
                                                          derive_validation_column="nonstrict_duplicatecheck"))

        if "is_numeric_validator_columns" in self._validators:
            validators.append(IsNumericValidator(columns=self._validators["is_numeric_validator_columns"],
                                                 description="[{0}] must be numeric".format("".join(str(e+"") for e in self._validators["is_numeric_validator_columns"])),
                                                 derive_validation_column="isnumericcheck"))

        if "is_int_validator_columns" in self._validators:
            validators.append(IsIntValidator(columns=self._validators["is_int_validator_columns"],
                                             description="[{0}] must be integer".format("".join(str(e+"") for e in self._validators["is_int_validator_columns"])),
                                             derive_validation_column="isintcheck"))

        if "nonstrict_duplicate_validator" in self._validators:
            validators.append(NonstrictDuplicateValidator(
                nonstrict_duplicate_validators_config=self._validators["nonstrict_duplicate_validator"],
                description="[{0}] must be unique (nonstrict)".format(
                    "".join(str(e + "") for e in self._validators["nonstrict_duplicate_validator"]["columns"]).replace(
                        "'", "''")),
                derive_validation_column="nonstrict_duplicatecheck"))

        if "is_numeric_validator_columns" in self._validators:
            validators.append(IsNumericValidator(columns=self._validators["is_numeric_validator_columns"],
                                                 description="[{0}] must be numeric".format("".join(str(e + "") for e in
                                                                                                    self._validators[
                                                                                                        "is_numeric_validator_columns"])),
                                                 derive_validation_column="isnumericcheck"))

        if "is_date_validator_columns" in self._validators:
            validators.append(IsDateValidator(columns=self._validators["is_date_validator_columns"],
                                             description="[{0}] must be date format".format("".join(
                                                 str(e + "") for e in self._validators["is_date_validator_columns"])),
                                             derive_validation_column="isdatecheck"))

        if "is_timestamp_validator_columns" in self._validators:
            validators.append(IsIntValidator(columns=self._validators["is_timestamp_validator_columns"],
                                             description="[{0}] must be timestamp".format("".join(
                                                 str(e + "") for e in self._validators["is_timestamp_validator_columns"])),
                                             derive_validation_column="isdatecheck"))


        if "is_timestamp_validator_columns" in self._validators:
            validators.append(IsIntValidator(columns=self._validators["is_timestamp_validator_columns"],
                                             description="[{0}] must be timestamp".format("".join(
                                                 str(e + "") for e in self._validators["is_timestamp_validator_columns"])),
                                             derive_validation_column="isdatecheck"))


        if "date_fomrat_validators" in self._validators:
            for column,date_format in self._validators["date_format_validators"].iteritems():
                validators.append(
                    DateFormatValidator(column=column, date_format=date_format,description="[{0}] must be match {1}".format("".join(str(e+"") for e in self._validators["date_format_validators"]),"".join(str("''"+e+"'' ")for e in date_format) if type(date_format)==list else date_format))
                )

        if "user_sql_validators" in self._validators:
            for derive_column,sql in self._validators["user_sql_validators"].iteritems():
                validators.append(UserSQLValidator(user_sql_predicate=sql,
                                                   derive_validation_column=derive_column,
                                                   description=derive_column))

        if "external_table_checks" in self._validators:
            for item in self._validators["external_table_checks"]:
                validators.append(ExternalTableValidator(spark=self.spark,
                                                         columns=item["columns"],
                                                         derive_validation_column=item["derive_validation_column"],
                                                         external_check_columns=item["external_check_columns"],
                                                         external_table_name=item["external_table_name"],
                                                         description="[{0}]" ))

        dfs["raw"]=self._trimming_columns(dfs["raw"])
        dq_checker=DataQualityCheck(run_date=self.run_date,validators=validators)
        df_clean,invalid_df=dq_checker.data_clean(dfs["raw"],self.spark)
        if self._ts_conversion_columns:
            df_clean=convert_datetimes_in_dataframe(df_clean,self._ts_conversion_columns)
        if self._date_conversion_columns:
            df_clean=convert_dates_in_dataframe(df_clean,self._date_conversion_columns)

        df_clean=self._formatting_schema(df_clean)
        if self._extra_columns_definition:
            df_clean=add_columns(df_clean,self._extra_columns_definition)
        return {"cleansed":df_clean,"quaraintine":invalid_df}

    def load(self,dfs):
        self._target_dir_t=fulfill_template(self,self._target_dir_t,default_layer=self._cleansed_layer)
        self._quarantine_path=fulfill_template(self,self._quarantine_path,default_layer=self._cleansed_layer)
        df_clean=dfs["cleansed"]
        self.simple_target_load(df_clean,add_proccess_dt=self._add_process_dt)
        self._rows=self.count_target()
        invalid_df=dfs["quarantine"].persist()
        if len(invalid_df.take(1))>0:
            self._dq_rejected_rows=invalid_df.count()
            invalid_df.write.save(path=self._quarantine_path,format="parquet",mode="overwrite")
            self.senf_mail_for_invalid_df(invalid_df)

    def _formatting_schema(self,df):
        system_columns={self._run_dt_col}
        for field,data_type in get_param(self.config,stage,"schema").item():
            if field not in system_columns:
                if __name__ == '__main__':
                    if __name__ == '__main__':
                        df=df.withColumn(field+"_new",df[field].cast(data_type)).drop(field)\
                            .withColumnRenamed(field+"_new",field)
        return df

    def trimming_columns(self,df):
        columns_to_trim=[item[0] for item in df.dtypes if item[1].startwith("string")]
        for col in columns_to_trim:
            df=df.withColumn(col,trim(df[col]))
        return df

    def _register_dfs(self,dfs):
        for name,dataframe in dfs.items():
            if dataframe:
                print("df {0} registered".format(name))
                dataframe.createOrReplaceTempView(name)
            else:
                print("WARNING: table {0} is None".format(name))

    def send_mail_for_invalid_df(self,invalid_df):
        mail_list=get_param_default(self.config,None,"common","mail_list")
        if isinstance(mail_list,dict) and mail_list.get("mail_list"):
            subject="warning email for {domain}_{entity} in {stage}".format(domain=self._domain,
                                                                            entity=self._entity,
                                                                            stage=stage
                                                                            )
            quarantine_message="Getting {} rows to quarantine".format(invalid_df.count())
            quarantine_message="\r\r\r\nSummary:"
            if self._fn_column in invalid_df.columns:
                summary_info_list=invalid_df.groupBy(invalid_df[self._fn_column],invalid_df["rule_description"]).count().collect()
                summary_template="\r\r\n in file:{file_name}, reason:{rule_description},count:{count}"
            else:
                summary_info_list=invalid_df.groupBy(invalid_df["rule_description"]).count().collect()
                summary_template="\r\r\n reason:{rule_description},count:{count}"

            for info in summary_info_list:
                quarantine_message+=summary_template.format(**info.asDict())

            send_email(cli_args=self.cli_args,mail_list=mail_list["mail_list"],subject=subject,body=quarantine_message)



