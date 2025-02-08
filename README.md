# SparkT

SparkT is an application demo using SparkSQL. Test for Bank Client

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install pyspark
```

## Prerequisite 
Using pySpark version 3.5.0

Using Python version 3.12.0

Using pip3 version 23.2.1 

Using Scala version 2.12.18

Using Java HotSpot(TM) 64-Bit Server VM, 21.0.2

see [requirement.txt](https://github.com/ruiiyang/sparkTest/blob/main/requirements.txt)

## Usage

```python
import PySpark
import Logging
```
Function
```
def filter_client_country
def rename 
```

Linux command example:

To have client_data output (in parque format) 
```
spark-submit main.py --country "France" --dataset1_path "dataset_one.csv" --dataset2_path "dataset_two.csv" 

```

Rotation-logging
```
spark-submit main.py --country "France" --dataset1_path "dataset_one.csv" --dataset2_path "dataset_two.csv" --conf spark.eventLog.rotation.enabled=true 
```

Result

```
Given argument of two dataset locations, able to join it to one dataset and masking the personal infoamation (name, credit card number) during this process. Output is a joined table in parquet format.
```

## Contributing

Pull requests are welcome. This code is only for project purpose.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
