import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)

#input will not be from another op but will be provided via the config_schema. 
#config schema will take in one parameter, a string name s3_key. The output of the op is a list of Stock.
#helper function provider csv_helper which takes in a file name and yields a generator of Stock using the class method for our custom data type.

@op(description="Ingest data from S3", config_schema={"s3_key": str}, out={"stocks": Out(dagster_type=List[Stock])})
def get_s3_data_op(context):
    return list(csv_helper(context.op_config["s3_key"]))

#require the output of the get_s3_data (which will be a list of Stock). The output of the process_data will be our custom type Aggregation
#processing occurring within the op will take the list of stocks and determine the Stock with the greatest high value. 

@op(description="Derive stock with greatest value.", ins={"stocks": In(dagster_type=List[Stock])}, out={"high_stock_data": Out(dagster_type=Aggregation)})
def process_data_op(context, stocks: list):
  high_stock = stocks[0].high       #set it to be the value from the first list
  high_stock_date = stocks[0].date  #set it to be the value from the first list
  for stock in stocks:
    if stock.high >= high_stock:
      high_stock = stock.high
      high_stock_date = stock.date
  return Aggregation(date= high_stock_date, high = high_stock)

#need to accept the Aggregation type from your process_data
@op(description="Send data to Redis.", ins={"high_stock_data": In(dagster_type=Aggregation)})
def put_redis_data_op(context, high_stock_data):
    pass

#need to accept the Aggregation type from your process_data
@op(description="Send transformed data to S3.", ins={"high_stock_data": In(dagster_type=Aggregation)})
def put_s3_data_op(context, high_stock_data):
    pass

#chain the ops together so that they execute in the correct order and correctly pass their outputs.

@job
def machine_learning_job():
    a = get_s3_data_op()
    b = process_data_op(a)
    c = put_redis_data_op(b)
    d = put_s3_data_op(b)