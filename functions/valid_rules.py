from pyspark.sql.functions import coalesce,when,count,lit,col,isnull,expr,trim,unix_timestamp,regexp_extract,max,min
from functools import reduce
from pyspark.sql import Window
from pyspark.sql.types import DecimalType,IntegerType,DateType,TimestampType

INT_REGEXP=r'^(-*)(\d+)(\.*)(0*)$'

def is_null_or_empty(column):
    return coalesce(trim(col(column)),lit(''))==''

def create_condition(columns):
    return reduce(lambda a,b:a|b,[is_null_or_empty(column) for column in columns])

def create_condition_type(columns,cast_to_type):
    return reduce(lambda a,b:a|b,[(~isnull(col(column))& isnull(col(column).cast(cast_to_type))) for column in columns])

def create_condition_type_with_regexp(columns,cast_to_type,regexp):
    return create_condition_type(columns,cast_to_type) \
        |reduce(lambda a,b:a|b,[(~isnull(col(column))& (regexp_extract(col(column),regexp,0)=='')) for column in columns])

def prepare_condition_case_type(columns,cast_to_type,regexp=None):
    condition_case=lit(None)
    if len(columns)!=0:
        check=create_condition_type_with_regexp(columns,cast_to_type,regexp) if regexp \
            else create_condition_type(columns,cast_to_type)
        condition_case=when(check,1).otherwise(None)
    return condition_case

def prepare_condition_with_decimal(columns):
    def check(decimal_columns):
        precision=38
        scale=10
        if type(decimal_columns)==tuple:
            name=decimal_columns[0]
            precision=decimal_columns[1]
            scale=decimal_columns[2]
        else:
            name=decimal_columns
        return ~isnull(col(name))&isnull(col(name).cast(DecimalType(precision,scale)))

    condition_case=lit(None)
    if len(columns)!=0:
        check=reduce(lambda a,b:a|b,[check(column) for column in columns])
        condition_case = when(check, "1").otherwise(None)
    return condition_case

def check_null_value(df,columns,derive_validate_column):
    conditon_case=lit(None)
    if len(columns)!=0:
        condition=create_condition(columns)
        conditon_case=when(condition, "1").otherwise(None)
    df=df.withColumn(derive_validate_column,conditon_case)
    return df

def check_duplicate_rows(df,columns,derive_validate_column):
    conditon_case = lit(None)
    if len(columns)!=0:
        exprs=[expr(column) for column in columns]
        df =df.select("*",count("*").over(Window.partitionBy(exprs)).alias("cnt"))
        conditon_case=when(df["cnt"]>1,"1").otherwise(None)
    df=df.withColumn(derive_validate_column,conditon_case)
    df.drop("cnt")
    return df

def nonstrict_duplicate_validator(df,columns,derive_validate_column,incremental_column,sorting_direction):
    conditon_case = lit(None)
    if len(columns) != 0:
        exprs = [expr(column) for column in columns]
        if sorting_direction=="max":
            aggregation_function=max(incremental_column).alias(incremental_column)
        else:
            aggregation_function=min(incremental_column).alias(incremental_column)
        max_value_incremental_column=df.groupBy(columns).agg(aggregation_function)
        max_value_incremental_column=max_value_incremental_column.drop(*columns)
        df=df.select("*",count("*").over(Window.partitionBy(exprs)).alias("cnt"))
        df=df.join(max_value_incremental_column,df[incremental_column]==max_value_incremental_column[incremental_column],"left_over")\
            .withColumn("cont_",when(max_value_incremental_column[incremental_column].isNotNull(),1).otherwise(df["cnt"])).distinct()
        conditon_case=when(df["cnt_"]>1,"1").otherwise(None)
        df=df.drop(max_value_incremental_column[incremental_column])
        df=df.withColumn(derive_validate_column,conditon_case)
        df=df.drop("cnt","cnt_")
        return df

def is_numeric(df,columns,derive_validate_column):
    return df.withColumn(derive_validate_column,prepare_condition_with_decimal(columns))

def is_int(df,columns,derive_validate_column):
    return df.withColumn(derive_validate_column,prepare_condition_case_type(columns,IntegerType(),regexp=INT_REGEXP))

def is_date(df,columns,derive_validate_column):
    return df.withColumn(derive_validate_column,prepare_condition_case_type(columns,DateType()))

def is_timestamp(df,columns,derive_validate_column):
    return df.withColumn(derive_validate_column,prepare_condition_case_type(columns,TimestampType()))

def do_user_sql_check(df,derive_validate_column,user_sql_predicate):
    expression="IF({},1,Null)".format(user_sql_predicate)
    return df.withColumn(derive_validate_column,expr(expression))

def do_external_table_check(spark,df,columns,derive_validate_column,external_check_columns,external_table_name):
    external_df=spark.table(external_table_name).select(external_check_columns).distinct()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")
    join_conditions=[]
    when_conditions=[]
    for original_column,external_column in list(zip(columns,external_check_columns)):
        join_conditions.append(df[original_column]==external_df[external_column])
        when_conditions.append(isnull(external_df[external_column]))
    join_expr=reduce(lambda a,b:a & b, join_conditions)
    where_expr=reduce(lambda a,b:a | b, when_conditions)
    df=df.join(external_df,join_expr,"left_outer")
    df=df.withColumn(derive_validate_column,when(where_expr,1).otherwise(None))
    for external_column in external_check_columns:
        df=df.drop(external_df[external_column])
    return df

def do_date_format_check(df,column,date_format,derive_validation_column):
    if isinstance(date_format,list):
        converted_column=coalesce(*[unix_timestamp(column, date_format_item) for date_format_item in date_format])
    else:
        converted_column=unix_timestamp(column,date_format)
    return df.withColumn(derive_validation_column,when(~isnull(column)&isnull(converted_column),"1").otherwise(None))


class DataValidator(object):
    def __init__(self,columns,derive_validation_column,description=""):
        self.derive_validation_column=derive_validation_column
        self.description=description
        self._columns=columns

    def run_check(self,df):
        pass

class NullValueValidator(DataValidator):
    def run_check(self,df):
        return check_null_value(df,self._columns,self.derive_validation_column)

class DuplicateRowValidator(DataValidator):
    def run_check(self,df):
        return check_duplicate_rows(df,self._columns,self.derive_validation_column)

class NonstrictDuplicateValidator(DataValidator):
    def __init__(self,nonstrict_duplicate_validators_config,description,derive_validation_column):
        columns=nonstrict_duplicate_validators_config["columns"]
        if "sorting_direction" in nonstrict_duplicate_validators_config:
            self.sorting_direction=nonstrict_duplicate_validators_config["sorting_direction"]
        else:
            self.sorting_direction="max"
        self.incremental_column=nonstrict_duplicate_validators_config["sorting_direction"]
        super(NonstrictDuplicateValidator,self).__init__(
            columns=columns,
            derive_validation_column=derive_validation_column,
            description=description
        )

    def run_check(self,df):
        return nonstrict_duplicate_validator(df,self._colomns,self.derive_validation_column,
                                             self.incremental_column,self.sorting_direction)


class IsNumericValidator(DataValidator):
    def run_check(self,df):
        return is_numeric(df,self._columns,self.derive_validation_column)

class IsIntValidator(DataValidator):
    def run_check(self,df):
        return is_int(df,self._columns,self.derive_validation_column)

class IsDateValidator(DataValidator):
    def run_check(self,df):
        return is_date(df,self._columns,self.derive_validation_column)

class IsTimestampValidator(DataValidator):
    def run_check(self,df):
        return is_timestamp(df,self._columns,self.derive_validation_column)

class DateFormatValidator(DataValidator):
    def __init__(self,column, date_format,description="date_format_check"):
        super(DateFormatValidator,self).__init__(
            columns=[column],
            derive_validation_column="date_format_check_{column}".format(column=column),
            description=description
        )
        self.date_format=date_format

    def run_check(self,df):
        return do_date_format_check(df, self.column[0],self.date_format,self.derive_validation_column)


class UserSQLValidator(DataValidator):
    def __init__(self, derive_validation_column,user_sql_predicate,description=''):
        super(DateFormatValidator, self).__init__(
            columns=None,
            derive_validation_column=derive_validation_column,
            user_sql_predicate=user_sql_predicate,
            description=description
        )

    def run_check(self,df):
        return do_user_sql_check(df,self.derive_validation_column,self.user_sql_predicate)

class ExternalTableValidator(DataValidator):
    def __init__(self,spark,column,derive_validation_column,external_check_columns,external_table_name,description=''):
        super(ExternalTableValidator,self).__init__(
            column=column,
            derive_validation_column=derive_validation_column,
            description=description
        )
        self._spark=spark
        self._external_check_columns=external_check_columns
        self._external_table_name=external_table_name

    def run_check(self,df):
        return do_external_table_check(self._spark,df,self._columns,self.derive_validation_column,self._external_check_columns,self._external_table_name)


