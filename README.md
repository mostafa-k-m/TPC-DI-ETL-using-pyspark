# TPC-DI ETL using pyspark

please visit http://www.tpc.org/tpcdi/ for more information about TPC-DI and to get a copy of the data.


```python
import os
os.environ["JAVA_HOME"] = "/usr/lib64/jvm/jre-1.8.0-openjdk"
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Project2 - ETL based on TPC-DI") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()
```

We will start by defining functions to parse the xml file. We will read the xml file as an rdd then map those functions to said rdd to produce data frames. we had to create a separate function to handle each action type, A function to handle new customers, a function to handle updates, adding accounts and closing accounts.

Each of these functions will produce a seperate rdd that we will later collect into dataframs


```python
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, DateType, TimestampType
```

`customer_parser` will handle new customers and will return all customer data and all account data


```python
import xml.etree.ElementTree as ET
def customer_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'NEW':
                record = []
                list_of_attributes = ['C_ID', 'C_TAX_ID', 'C_GNDR', 'C_TIER', 'C_DOB']
                for attribute in list_of_attributes:
                    try:
                        record.append(Customer.attrib[attribute])
                    except:
                        record.append(None)
                for Element in Customer:
                    if Element.tag == 'ContactInfo':
                        for Subelement in Element:
                            if Subelement.tag[:-1] == 'C_PHONE_':
                                phone_number = ''
                                for Subsubelement in Subelement:
                                    if isinstance(Subsubelement.text, str):                                
                                        phone_number += Subsubelement.text + " "
                                if len(phone_number)>1:
                                    phone_number = phone_number[:-1]
                                else:
                                    phone_number = None
                                record.append(phone_number)
                            else:
                                record.append(Subelement.text)
                    elif Element.tag == 'Account':
                        for attribute in Element.attrib.values():
                            record.append(attribute)
                        for Subelement in Element:
                            record.append(Subelement.text)
                    else:
                        for Subelement in Element:
                            record.append(Subelement.text)
                records.append(record)
    return records

```

`add_account_parser` will handle 'ADDACCT' action type and will return the account data and the customer's ID.


```python
def add_account_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'ADDACCT':
                record = []
                record.append(Customer.attrib['C_ID'])
                for Element in Customer:
                    if Element.tag == 'Account':
                        for attribute in Element.attrib.values():
                            record.append(attribute)
                        for Subelement in Element:
                            record.append(Subelement.text)
                records.append(record)
    return records
```

`update_customer_parser` will handle 'UPDCUST' actions. We noticed that each update will have only the new fields. for example a customer could update their email but keep every thing else the same. our parser will return every field for each customer but will have None in fields that did not get updated.


```python
def update_customer_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'UPDCUST':
                record = []
                list_of_attributes = ['C_ID', 'C_TAX_ID', 'C_GNDR', 'C_TIER', 'C_DOB']
                for attribute in list_of_attributes:
                    try:
                        record.append(Customer.attrib[attribute])
                    except:
                        record.append(None)
                for Element in Customer:
                    dict={
                    "C_L_NAME":None,
                    "C_F_NAME":None,
                    "C_M_NAME":None,
                    'C_ADLINE1':None,
                    'C_ADLINE2':None,
                    'C_ZIPCODE':None,
                    'C_CITY':None,
                    'C_STATE_PROV':None,
                    'C_CTRY':None,
                    'C_PRIM_EMAIL':None,
                    'C_ALT_EMAIL':None,
                    'C_PHONE_1':None,
                    'C_PHONE_2':None,
                    'C_PHONE_3':None,
                    "C_LCL_TX_ID":None,
                    "C_NAT_TX_ID":None
                    }
                    if Element.tag == 'ContactInfo':
                        for Subelement in Element:
                            if Subelement.tag[:-1] == 'C_PHONE_':
                                phone_number = ''
                                for Subsubelement in Subelement:
                                    if isinstance(Subsubelement.text, str):                                
                                        phone_number += Subsubelement.text + " "
                                if len(phone_number)>1:
                                    phone_number = phone_number[:-1]
                                else:
                                    phone_number = None
                                dict[Subelement.tag] = phone_number
                            else:
                                dict[Subelement.tag] = Subelement.text
                    elif Element.tag == 'Account':
                        continue
                    else:
                        for Subelement in Element:
                            dict[Subelement.tag] = Subelement.text
                records.append(record+list(dict.values()))
    return records
```

`update_account_parser` will handle 'UPDACCT', no need to return None here because the account data is always complete.


```python
def update_account_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'UPDACCT':
                record = []
                record.append(Customer.attrib['C_ID'])
                for Account in Customer:
                    record.append(Account.attrib['CA_ID'])
                    try:
                        record.append(Account.attrib['CA_TAX_ST'])
                    except:
                        record.append(None)
                        dict = {
                        "CA_B_ID":None,
                        "CA_NAME":None}
                    for element in Account:
                        dict[element.tag] = element.text
                records.append(record+list(dict.values()))
    return records
```

We will consider 'INACT' and 'CLOSEACCT' both 'INACT' for simplicity's sake.


```python
def inactive_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'INACT' or ActionType == 'CLOSEACCT':
                records.append(Customer.attrib['C_ID'])
    return records
```

We Will read the xml file into an rdd


```python
file_rdd = spark.read.text('./Dataset/CustomerMgmt.xml', wholetext=True).rdd
```

we will create `new_customer_records_rdd` by using `customer_parser`


```python
new_customer_records_rdd = file_rdd.flatMap(customer_parser)
```

`customer_parser` returns for each new customer, the customer details and their account details. we need to split the resulting dataframe to separate the customers' information from the accounts' information


```python
new_customer_schema = StructType([
    StructField("C_ID", StringType(), False),
    StructField("C_TAX_ID", StringType(), False),
    StructField("C_GNDR", StringType(), True),
    StructField("C_TIER", StringType(), True),
    StructField("C_DOB", StringType(), False),
    StructField("C_L_NAME", StringType(), False),
    StructField("C_F_NAME", StringType(), False),
    StructField("C_M_NAME", StringType(), True),
    StructField("C_ADLINE1", StringType(), False),
    StructField("C_ADLINE2", StringType(), True),
    StructField("C_ZIPCODE", StringType(), False),
    StructField("C_CITY", StringType(), False),
    StructField("C_STATE_PROV", StringType(), False),
    StructField("C_CTRY", StringType(), False),
    StructField("C_PRIM_EMAIL", StringType(), False),
    StructField("C_ALT_EMAIL", StringType(), True),
    StructField("C_PHONE_1", StringType(), True),
    StructField("C_PHONE_2", StringType(), True),
    StructField("C_PHONE_3", StringType(), True),
    StructField("C_LCL_TX_ID", StringType(), False),
    StructField("C_NAT_TX_ID", StringType(), False),
    StructField("CA_ID", StringType(), False),
    StructField("CA_TAX_ST", StringType(), False),
    StructField("CA_B_ID", StringType(), False),
    StructField("CA_NAME", StringType(), True)])
customer_schema = StructType([
    StructField("C_ID", StringType(), True),
    StructField("C_TAX_ID", StringType(), True),
    StructField("C_GNDR", StringType(), True),
    StructField("C_TIER", StringType(), True),
    StructField("C_DOB", StringType(), True),
    StructField("C_L_NAME", StringType(), True),
    StructField("C_F_NAME", StringType(), True),
    StructField("C_M_NAME", StringType(), True),
    StructField("C_ADLINE1", StringType(), True),
    StructField("C_ADLINE2", StringType(), True),
    StructField("C_ZIPCODE", StringType(), True),
    StructField("C_CITY", StringType(), True),
    StructField("C_STATE_PROV", StringType(), True),
    StructField("C_CTRY", StringType(), True),
    StructField("C_PRIM_EMAIL", StringType(), True),
    StructField("C_ALT_EMAIL", StringType(), True),
    StructField("C_PHONE_1", StringType(), True),
    StructField("C_PHONE_2", StringType(), True),
    StructField("C_PHONE_3", StringType(), True),
    StructField("C_LCL_TX_ID", StringType(), True),
    StructField("C_NAT_TX_ID", StringType(), True)])
account_schema = StructType([
    StructField("C_ID", StringType(), True),
    StructField("CA_ID", StringType(), True),
    StructField("CA_TAX_ST", StringType(), True),
    StructField("CA_B_ID", StringType(), True),
    StructField("CA_NAME", StringType(), True)])
new_customer_df = new_customer_records_rdd.toDF(new_customer_schema).select("C_ID", "C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB", "C_L_NAME", "C_F_NAME", "C_M_NAME", "C_ADLINE1", "C_ADLINE2", "C_ZIPCODE", "C_CITY", "C_STATE_PROV", "C_CTRY", "C_PRIM_EMAIL", "C_ALT_EMAIL", "C_PHONE_1", "C_PHONE_2", "C_PHONE_3", "C_LCL_TX_ID", "C_NAT_TX_ID")
new_account_df = new_customer_records_rdd.toDF(new_customer_schema).select("C_ID", "CA_ID", "CA_TAX_ST", "CA_B_ID", "CA_NAME")
```

We now have two dataframes containing new customers' data and their accounts' data respectively.


```python
new_customer_df.show(5)
```

    +----+-----------+------+------+----------+--------+--------+--------+--------------------+---------+---------+----------------+------------+--------------------+--------------------+--------------------+------------------+------------+---------+-----------+-----------+
    |C_ID|   C_TAX_ID|C_GNDR|C_TIER|     C_DOB|C_L_NAME|C_F_NAME|C_M_NAME|           C_ADLINE1|C_ADLINE2|C_ZIPCODE|          C_CITY|C_STATE_PROV|              C_CTRY|        C_PRIM_EMAIL|         C_ALT_EMAIL|         C_PHONE_1|   C_PHONE_2|C_PHONE_3|C_LCL_TX_ID|C_NAT_TX_ID|
    +----+-----------+------+------+----------+--------+--------+--------+--------------------+---------+---------+----------------+------------+--------------------+--------------------+--------------------+------------------+------------+---------+-----------+-----------+
    |   0|923-54-6498|     F|     3|1940-12-02| Joannis|   Adara|    null|     4779 Weller Way|     null|    92624|        Columbus|     Ontario|              Canada|Adara.Joannis@moo...|Adara.Joannis@gmx...|    1 872 523-8928|    492-3961|     null|        CA3|        YT3|
    |   1|645-68-9627|     F|     3|1982-12-17| Paperno|  Jirina|       P|   7216 Gates Avenue|     null|  H5K 1Q9|       Inglewood|          WI|United States of ...|Jirina.P.Paperno@...|Jirina.P.Paperno@...|          767-4707|        null|     null|        BC6|        NU7|
    |   2|332-28-3838|  null|     3|1994-06-08| McBryan|  Mariam|    null|   12566 Misty Upper|     null|    78220|        Berkeley|Saskatchewan|United States of ...|Mariam.McBryan@ag...|Mariam.McBryan@sh...|420 757-3642 61998|    811-7498|     null|        NY4|        IA1|
    |   3|472-49-1339|     B|     3|1944-09-01|    Adey| Robinia|       L|12513 Wesleyan Bo...|     null|  H2C 1Z2|            Hull|          AL|United States of ...|Robinia.L.Adey@12...|Robinia.L.Adey@ic...|    1 819 163-0774|777 787-1085|     null|        WI4|        MO1|
    |   4|700-39-4024|     m|  null|1999-09-22| Haubert|    Lulu|    null| 6066 Fillmore Lower|     null|    10044|Rancho Cucamonga|          OR|United States of ...|Lulu.Haubert@buff...|                null|          734-4072|    713-2893|     null|        ON4|        MB7|
    +----+-----------+------+------+----------+--------+--------+--------+--------------------+---------+---------+----------------+------------+--------------------+--------------------+--------------------+------------------+------------+---------+-----------+-----------+
    only showing top 5 rows
    



```python
new_account_df.show(5)
```

    +----+-----+---------+-------+--------------------+
    |C_ID|CA_ID|CA_TAX_ST|CA_B_ID|             CA_NAME|
    +----+-----+---------+-------+--------------------+
    |   0|    0|        1|  17713|CJlmMuFyibKOmKLHI...|
    |   1|    1|        2|    615|BbxTgVGOlgyrYtVRj...|
    |   2|    2|        1|   3927|IGzIDNTTRUDKwGaoV...|
    |   3|    3|        1|   6256|ZHXwHtCcLZqdWhWOP...|
    |   4|    4|        1|   3412|mzlYZlTIDmOGuKQHO...|
    +----+-----+---------+-------+--------------------+
    only showing top 5 rows
    


We will create `add_account_records_rdd` using `add_account_parser` and will create another dataframes containing accounts that existing customers added.


```python
add_account_records_rdd = file_rdd.flatMap(add_account_parser)
add_account_df = add_account_records_rdd.toDF(account_schema)
add_account_df.show(5)
```

    +----+-----+---------+-------+--------------------+
    |C_ID|CA_ID|CA_TAX_ST|CA_B_ID|             CA_NAME|
    +----+-----+---------+-------+--------------------+
    |  88|  125|        2|  12126|WUIiLVBcUKKmgobPO...|
    |  54|  126|        1|  15244|JEddDkBzL R NXaer...|
    |  20|  127|        0|  12314|fFWYxTIiUmlHCHaCc...|
    |  21|  128|        1|  25910|PDQySQaONlBUACNtV...|
    |  77|  129|        1|  43888|ASPUxCpSHDPGGNQva...|
    +----+-----+---------+-------+--------------------+
    only showing top 5 rows
    


We will create `updated_account_rdd` with `update_account_parser` and create a dataframe that contains all the acounts that have been updated


```python
from pyspark.sql.functions import udf
updated_account_rdd = file_rdd.flatMap(update_account_parser)
updated_account_df = updated_account_rdd.toDF(account_schema)
```

Finally, to collect all the accounts in one dataframe we will do the following:
- We concatenate `new_account_df` and `add_account_df`, to get all the initial accounts data in one dataframe.
- We will do a left anti join between the new accounts and the updated accounts to get only the accounts that have not been updated.
- Then we will concatenate the result with the accounts that have been updated. the resulting dataframe will have all up to date account data.


```python
Accounts = new_account_df.union(add_account_df).join(updated_account_df, on=['C_ID','CA_ID'], how='left_anti').union(updated_account_df)
```

Finally for the accounts, we will populate the `CA_ST_ID` column


```python
inactive_accounts = file_rdd.flatMap(inactive_parser)
inact_list = inactive_accounts.collect()
inact_func = udf(lambda x: 'INAC' if str(x) in inact_list else 'ACTV')

Accounts = Accounts.withColumn('CA_ST_ID', inact_func(Accounts.C_ID))
Accounts.show(5)
```

    +-----+-----+---------+-------+--------------------+--------+
    | C_ID|CA_ID|CA_TAX_ST|CA_B_ID|             CA_NAME|CA_ST_ID|
    +-----+-----+---------+-------+--------------------+--------+
    |   10| 3429|        1|   4984|NvnqmafKEeRraHJlD...|    INAC|
    |10284|20469|        0|  30262|WSrAJPnvZzbENxGPc...|    ACTV|
    |10472|20832|        1|   7082|GxHMKBqZhsFTwZxrB...|    ACTV|
    |  106|  916|        1|  27782|fXyDCcGSMkKqkcAJD...|    INAC|
    |11165|22225|        1|  12877|i HGYvIGvIsbtW KO...|    ACTV|
    +-----+-----+---------+-------+--------------------+--------+
    only showing top 5 rows
    


Now back to customers, we will create a dataframe that contains all the customers who updated their data.


```python
update_customer_rdd = file_rdd.flatMap(update_customer_parser)
update_customer_df = update_customer_rdd.toDF(customer_schema)
update_customer_df.show(5)
```

    +----+--------+------+------+-----+--------+--------+--------+---------+---------+---------+------+------------+------+--------------------+--------------------+--------------+--------------+------------------+-----------+-----------+
    |C_ID|C_TAX_ID|C_GNDR|C_TIER|C_DOB|C_L_NAME|C_F_NAME|C_M_NAME|C_ADLINE1|C_ADLINE2|C_ZIPCODE|C_CITY|C_STATE_PROV|C_CTRY|        C_PRIM_EMAIL|         C_ALT_EMAIL|     C_PHONE_1|     C_PHONE_2|         C_PHONE_3|C_LCL_TX_ID|C_NAT_TX_ID|
    +----+--------+------+------+-----+--------+--------+--------+---------+---------+---------+------+------------+------+--------------------+--------------------+--------------+--------------+------------------+-----------+-----------+
    |  69|    null|  null|     2| null|    null|    null|    null|     null|     null|     null|  null|        null|  null|Venus.DiLoreto@ip...|                null|1 889 662-1131|      505-9373|              null|       null|       null|
    |   7|    null|  null|     3| null|    null|    null|    null|     null|     null|     null|  null|        null|  null|Riyad.Ayukawa@eur...|Riyad.Ayukawa@Sof...|      837-8927|672-0894 74696|    1 893 482-1103|       null|       null|
    |  35|    null|  null|     1| null|    null|    null|    null|     null|     null|     null|  null|        null|  null|                null|                null|  790 829-6136|          null|              null|       null|       null|
    |   0|    null|  null|  null| null|    null|    null|    null|     null|     null|     null|  null|        null|  null|Adara.Joannis@gma...|                null|  357 423-9191|  270 141-3475|421 277-2023 59348|       null|       null|
    |  28|    null|  null|     3| null|    null|    null|    null|     null|     null|     null|  null|        null|  null|  Dido.Harris@gmx.it|                null|      882-2241|  707 384-6361|              null|       null|       null|
    +----+--------+------+------+-----+--------+--------+--------+---------+---------+---------+------+------------+------+--------------------+--------------------+--------------+--------------+------------------+-----------+-----------+
    only showing top 5 rows
    


We will do a left anti join between `new_customer_df` and `update_customer_df` to get all the customers whose information did not get updated in one dataframe. We will then do an inner join between `new_customer_df` and `update_customer_df`to get all customers whose information has been updated. The resulting datafram will have, for each customer, one value for ID and two values for each other field. one coming from the data before the update and one from after. The data from after the update is incomplete and will contain null values if a certain feild did not get updated.

We will rename the second dataframes columns for clarity by appending '_pdate' to the updated columns. for example "phone1" and "phone1_update"


```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
customers_not_updated = new_customer_df.join(update_customer_df, on=['C_ID'], how='left_anti')
customers_updated = new_customer_df.join(update_customer_df, on=['C_ID'], how='inner')
columns = []
for index, column in enumerate(customers_updated.columns):
    if index <= 20:
        columns.append(column)
    else:
        columns.append(column+'_update')
```

We now need to iterate over each row to check if each field has been updated (if the new value is not null) and replace the old value with the new value. to achieve this we will convert the dataframe back to rdd then use `customer_updater` to insert the updated values and we will get a dataframe that will have the up to date data.


```python
customers_updated = customers_updated.toDF(*columns).rdd
def customer_updater(row):
    new_row= [row.C_ID]
    for column in columns:
        if column != 'C_ID' and (not '_update' in column):
            if not getattr(row,column+'_update') is None:
                new_row.append(getattr(row,column+'_update'))
            else:
                new_row.append(getattr(row,column))
    return new_row
customers_updated = customers_updated.map(customer_updater).toDF(customer_schema)
customers_updated.show(5)
```

    +-----+-----------+------+------+----------+---------+--------+--------+--------------------+---------+---------+------------+------------+--------------------+--------------------+--------------------+--------------+------------+---------+-----------+-----------+
    | C_ID|   C_TAX_ID|C_GNDR|C_TIER|     C_DOB| C_L_NAME|C_F_NAME|C_M_NAME|           C_ADLINE1|C_ADLINE2|C_ZIPCODE|      C_CITY|C_STATE_PROV|              C_CTRY|        C_PRIM_EMAIL|         C_ALT_EMAIL|     C_PHONE_1|   C_PHONE_2|C_PHONE_3|C_LCL_TX_ID|C_NAT_TX_ID|
    +-----+-----------+------+------+----------+---------+--------+--------+--------------------+---------+---------+------------+------------+--------------------+--------------------+--------------------+--------------+------------+---------+-----------+-----------+
    |10436|925-79-3935|  null|     3|2005-01-15|   Hachey|  Sarath|    null|196 Bellaire Cres...|     null|    19244|Fort Collins|          NM|United States of ...|Sarath.Hachey@icq...|Sarath.Hachey@gmx...|743-0141 32615|957 649-7453| 769-5940|        OH8|       MT10|
    |11078|186-47-5873|     f|     3|1946-04-15|   Namiki|   Robin|       M|23710 Amsterdam B...|     null|  E7O 1M3|  Pittsburgh|          AL|United States of ...|Robin.M.Namiki@ma...|Robin.M.Namiki@hu...|      464-1326|    722-1040|     null|        OK6|        AR6|
    |11078|186-47-5873|     f|     3|1946-04-15|   Namiki|   Robin|       M|23710 Amsterdam B...|     null|  E7O 1M3|  Pittsburgh|          AL|United States of ...|Robin.M.Namiki@en...|Robin.M.Namiki@my...|      444-8396|    722-1040|     null|        OK6|        AR6|
    | 1436|604-00-3391|  null|     3|2014-08-19|    Hadel|Surinder|       E|   15079 Decatur Rue|     null|    80902|  Pittsburgh|          MT|              Canada|Surinder.E.Hadel@...|Surinder.E.Hadel@...|      349-3359|    704-1650|     null|        OK5|        AR5|
    | 2088|241-27-2005|  null|     2|1939-09-10|Bessuille|    Sriv|       I|  17261 Alpine Upper|     null|  B1C 1X8| West Valley|          FL|              Canada|Sriv.I.Bessuille@...|Sriv.I.Bessuille@...|      698-6703|    173-8642|     null|        CA3|        SK4|
    +-----+-----------+------+------+----------+---------+--------+--------+--------------------+---------+---------+------------+------------+--------------------+--------------------+--------------------+--------------+------------+---------+-----------+-----------+
    only showing top 5 rows
    


Finally we will concatenate the dataframe containing customers who did not get updated and the dataframe containing customers who did get updated.


```python
Customers = customers_not_updated.union(customers_updated)
Customers.show(5)
```

    +-----+-----------+------+------+----------+--------+--------+--------+--------------------+---------+---------+---------------+--------------------+--------------------+--------------------+--------------------+------------------+------------+--------------+-----------+-----------+
    | C_ID|   C_TAX_ID|C_GNDR|C_TIER|     C_DOB|C_L_NAME|C_F_NAME|C_M_NAME|           C_ADLINE1|C_ADLINE2|C_ZIPCODE|         C_CITY|        C_STATE_PROV|              C_CTRY|        C_PRIM_EMAIL|         C_ALT_EMAIL|         C_PHONE_1|   C_PHONE_2|     C_PHONE_3|C_LCL_TX_ID|C_NAT_TX_ID|
    +-----+-----------+------+------+----------+--------+--------+--------+--------------------+---------+---------+---------------+--------------------+--------------------+--------------------+--------------------+------------------+------------+--------------+-----------+-----------+
    |10096|214-25-1030|  null|     3|1997-09-15|  Polder|    Kyla|    null|    15031 Wood South|     null|    29409|          Omaha|                  CA|United States of ...|  Kyla.Polder@gmx.hk|                null|      533 560-6464|950 140-4160|1 217 452-4683|        OK4|        AR5|
    |10351|461-07-0288|  null|     3|1981-04-02|   Perez|Germaine|       J|    24160 Star Drive|     null|    66624| San Bernardino|Prince Edward Island|United States of ...|Germaine.J.Perez@...|                null|          084-5037|        null|          null|        IA5|        MT9|
    | 1090|668-12-9960|  null|     2|2011-12-08| Grisoni|   Steve|    null|22820 Wilshire Bo...|     null|    33108|Fort Lauderdale|                  NC|United States of ...|Steve.Grisoni@sht...|Steve.Grisoni@gmx.sg|          200-8103|530 132-0772|          null|        MT3|        WY1|
    |11332|460-09-5021|     f|     3|1974-03-16|  Cawley|  Dulcie|       K|4953 Williams Bou...|     null|    93792| Corpus Christi|                  SC|United States of ...|Dulcie.K.Cawley@p...|Dulcie.K.Cawley@l...|421 969-8302 70924|        null|1 324 726-9321|        CA3|        MO6|
    |11563|972-84-0885|     M|     3|1982-07-02|   Adcox|     Viv|    null|        3020 Fox Rue|     null|    33613|          Sandy|                  OK|United States of ...|Viv.Adcox@soodoni...|Viv.Adcox@letterb...|    1 536 315-5937|        null|          null|        NT8|        CA3|
    +-----+-----------+------+------+----------+--------+--------+--------+--------------------+---------+---------+---------------+--------------------+--------------------+--------------------+--------------------+------------------+------------+--------------+-----------+-----------+
    only showing top 5 rows
    



```python
 schema_TaxRate = StructType([
    StructField("TX_ID", StringType(), False),
    StructField("TX_NAME", StringType(), False),
    StructField("TX_RATE", FloatType(), False)])

df_TaxRate = spark.read\
            .format("csv")\
            .schema(schema_TaxRate)\
            .option("header", "false")\
            .option("sep", "|")\
            .load("./Dataset/TaxRate.txt")
df_TaxRate.show(5)
```

    +-----+--------------------+-------+
    |TX_ID|             TX_NAME|TX_RATE|
    +-----+--------------------+-------+
    |  US1|U.S. Income Tax B...|   0.15|
    |  US2|U.S. Income Tax B...|  0.275|
    |  US3|U.S. Income Tax B...|  0.305|
    |  US4|U.S. Income Tax B...|  0.355|
    |  US5|U.S. Income Tax B...|  0.391|
    +-----+--------------------+-------+
    only showing top 5 rows
    



```python
schema_Prospect = StructType([
    StructField("AgencyID", StringType(), False),
    StructField("LastName", StringType(), False),
    StructField("FirstName", StringType(), False),
    StructField("MiddleInitial", StringType(), False),
    StructField("Gender", StringType(), False),
    StructField("AddressLine1", StringType(), False),
    StructField("AddressLine2", StringType(), False),
    StructField("PostalCode", StringType(), False),
    StructField("City", StringType(), False),
    StructField("State", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("Phone", StringType(), False),
    StructField("Income", IntegerType(), False),
    StructField("NumberCars", IntegerType(), False),
    StructField("NumberChildren", IntegerType(), False),
    StructField("MaritalStatus", StringType(), False),
    StructField("Age", IntegerType(), False),
    StructField("CreditRating", IntegerType(), False),
    StructField("OwnOrRentFlag", StringType(), False),
    StructField("Employer", StringType(), False),
    StructField("NumberCreditCards", IntegerType(), False),
    StructField("NetWorth", IntegerType(), False)])

df_Prospect = spark.read\
            .format("csv")\
            .schema(schema_Prospect)\
            .option("header", "false")\
            .option("sep", ",")\
            .load("./Dataset/Prospect.csv")
df_Prospect.show(5)
```

    +--------+--------+---------+-------------+------+--------------------+------------+----------+---------------+-----+--------------------+--------------+------+----------+--------------+-------------+----+------------+-------------+--------------------+-----------------+--------+
    |AgencyID|LastName|FirstName|MiddleInitial|Gender|        AddressLine1|AddressLine2|PostalCode|           City|State|             Country|         Phone|Income|NumberCars|NumberChildren|MaritalStatus| Age|CreditRating|OwnOrRentFlag|            Employer|NumberCreditCards|NetWorth|
    +--------+--------+---------+-------------+------+--------------------+------------+----------+---------------+-----+--------------------+--------------+------+----------+--------------+-------------+----+------------+-------------+--------------------+-----------------+--------+
    |    KOZ0|  KOZIOL|  Mahmood|         null|  null|17886 st. phillip...|        null|   H6b 1w1|       St. Paul|   TN|              Canada|1-712-522-6088|368776|      null|             3|            W|  20|         760|            O|             Brink's|             null| 1058868|
    |    DIS1|DISCOVER|     ROEL|            J|  null|5539 carrington west|        null|     43602|       Columbia|   CO|United States of ...|1-626-426-4298|177967|         5|             1|            U|   3|         555|            U|                null|                6| 1988185|
    |    rom2|  romano|    brear|         null|     m|13031 lendon cres...|    apt. 180|   E7n 1p8|North Las Vegas|   WY|United States of ...|1-524-787-8784|321772|         2|             1|            S|null|         566|            O|                null|                6| 3673128|
    |    SCH3|SCHINKEL|      Bev|         null|  null|   22594 reagan park|        null|     13222|       Victoria|   MT|United States of ...|1-741-997-1688| 25449|         2|             1|            W|  77|        null|            O|Air Products & Ch...|                3| 2005895|
    |    BIS4|   BISCH|  analise|         null|     M|4833 baltic boule...|        null|   h5i 1o4|        Detroit|   MT|United States of ...|1-002-194-1991|166567|         0|             3|            M|  21|         815|            O|             Oshkosh|                4|  624736|
    +--------+--------+---------+-------------+------+--------------------+------------+----------+---------------+-----+--------------------+--------------+------+----------+--------------+-------------+----+------------+-------------+--------------------+-----------------+--------+
    only showing top 5 rows
    



```python
from pyspark.sql.functions import broadcast
df_TaxRate_broad = broadcast(df_TaxRate)
df_TaxRate_broad.createOrReplaceTempView("TaxRate_broad")
```


```python
df_Prospect_broad = broadcast(df_Prospect)
df_Prospect_broad.createOrReplaceTempView("Prospect_broad")
```


```python
schema_Account = StructType([
    StructField("CDC_FLAG", StringType(), False),
    StructField("CDC_DSN", IntegerType(), True),
    StructField("CA_ID", IntegerType(), True),
    StructField("CA_B_ID", IntegerType(), True),
    StructField("CA_C_ID", IntegerType(), True),
    StructField("CA_NAME", StringType(), False),
    StructField("CA_TAX_ST", IntegerType(), True),
    StructField("CA_ST_ID", StringType(), True)])
```


```python
df_StatusType = spark.read\
            .format("csv")\
            .schema('ST_ID string, ST_NAME string')\
            .option("header", "false")\
            .option("sep", "|")\
            .load("./Dataset/StatusType.txt")
```


```python
df_StatusType.show(10)
```

    +-----+---------+
    |ST_ID|  ST_NAME|
    +-----+---------+
    | ACTV|   Active|
    | CMPT|Completed|
    | CNCL| Canceled|
    | PNDG|  Pending|
    | SBMT|Submitted|
    | INAC| Inactive|
    +-----+---------+
    



```python
df_StatusType_broad = broadcast(df_StatusType)
```


```python
from datetime import datetime
```


```python
Accounts.createOrReplaceTempView("accounts")
df_StatusType_broad.createOrReplaceTempView("statusType_broad")
```


```python
dimAccount = spark.sql("\
                       Select CA_ID as AccountID,\
                       C_ID as CustomerID,\
                       ST_NAME as Status,\
                       CA_NAME as AccountDesc,\
                       CA_TAX_ST as TaxStatus,\
                       CAST('True' as BOOLEAN) as IsCurrent,\
                       CAST('1' as INT) as BatchID,\
                       to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate,\
                       to_date('9999-12-31', 'yyyy-MM-dd') as EndDate\
                       From accounts join statusType_broad on accounts.CA_ST_ID = statusType_broad.ST_ID")
```


```python
dimAccount.show(10)
```

    +---------+----------+--------+--------------------+---------+---------+-------+-------------+----------+
    |AccountID|CustomerID|  Status|         AccountDesc|TaxStatus|IsCurrent|BatchID|EffectiveDate|   EndDate|
    +---------+----------+--------+--------------------+---------+---------+-------+-------------+----------+
    |     3429|        10|Inactive|NvnqmafKEeRraHJlD...|        1|     true|      1|   2015-01-01|9999-12-31|
    |    20469|     10284|  Active|WSrAJPnvZzbENxGPc...|        0|     true|      1|   2015-01-01|9999-12-31|
    |    20832|     10472|  Active|GxHMKBqZhsFTwZxrB...|        1|     true|      1|   2015-01-01|9999-12-31|
    |      916|       106|Inactive|fXyDCcGSMkKqkcAJD...|        1|     true|      1|   2015-01-01|9999-12-31|
    |    22225|     11165|  Active|i HGYvIGvIsbtW KO...|        1|     true|      1|   2015-01-01|9999-12-31|
    |    28436|     11167|  Active|WmgHiXrLf kNTUXSI...|        1|     true|      1|   2015-01-01|9999-12-31|
    |    25613|     11338|  Active|ZFqDhUqNLjCjSdCsd...|        2|     true|      1|   2015-01-01|9999-12-31|
    |    22638|     11368|  Active|fUkAPQJeRLJEYsswr...|        1|     true|      1|   2015-01-01|9999-12-31|
    |    12519|      1147|  Active|ciHrvUfyCUpsAqFHm...|        1|     true|      1|   2015-01-01|9999-12-31|
    |    22869|     11494|  Active|FHaCaIweDYOreJPk ...|        1|     true|      1|   2015-01-01|9999-12-31|
    +---------+----------+--------+--------------------+---------+---------+-------+-------------+----------+
    only showing top 10 rows
    



```python
dimAccount.printSchema()
```

    root
     |-- AccountID: string (nullable = true)
     |-- CustomerID: string (nullable = true)
     |-- Status: string (nullable = true)
     |-- AccountDesc: string (nullable = true)
     |-- TaxStatus: string (nullable = true)
     |-- IsCurrent: boolean (nullable = true)
     |-- BatchID: integer (nullable = true)
     |-- EffectiveDate: date (nullable = true)
     |-- EndDate: date (nullable = true)
    



```python
Customers.createOrReplaceTempView("customers")
```


```python
dimCustomer = spark.sql("\
                       Select c.C_ID as CustomerID,\
                       C_TAX_ID as TaxID,\
                       C_L_NAME as LastName,\
                       C_F_NAME as FirstName,\
                       C_M_NAME as MiddleInitial,\
                       C_GNDR as Gender,\
                       C_TIER as Tier,\
                       C_DOB as DOB,\
                       C_ADLINE1 as AddressLine1,\
                       C_ADLINE2 as AddressLine2,\
                       C_ZIPCODE as PostalCode,\
                       C_CITY as City,\
                       C_STATE_PROV as StateProv,\
                       C_CTRY as Country,\
                       C_PHONE_1 as Phone1,\
                       C_PHONE_2 as Phone2,\
                       C_PHONE_3 as Phone3,\
                       C_PRIM_EMAIL as Email1,\
                       C_ALT_EMAIL as Email2,\
                       NAT.TX_NAME as NationalTaxRateDesc,\
                       NAT.TX_RATE as NationalTaxRate,\
                       LCL.TX_NAME as LocalTaxRateDesc,\
                       LCL.TX_RATE as LocalTaxRate,\
                       AgencyID as AgencyID,\
                       CreditRating as CreditRating,\
                       NetWorth as NetWorth,\
                        COALESCE(CASE \
                            WHEN NetWorth > 1000000 THEN 'HighValue+' \
                            ELSE NULL \
                        END,\
                       CASE \
                            WHEN NumberChildren > 3 THEN 'Expenses+' \
                            WHEN NumberCreditCards > 5 THEN 'Expenses+'\
                            ELSE NULL \
                        END,\
                       CASE \
                            WHEN Age > 45 THEN 'Boomer+' \
                            ELSE NULL \
                        END,\
                       CASE \
                            WHEN Income < 50000 THEN 'MoneyAlert+' \
                            WHEN CreditRating < 600 THEN 'MoneyAlert+' \
                            WHEN NetWorth < 100000 THEN 'MoneyAlert+' \
                            ELSE Null \
                        END,\
                       CASE \
                            WHEN NumberCars > 3 THEN 'Spender+' \
                            WHEN NumberCreditCards > 7 THEN 'Spender+' \
                            ELSE Null \
                        END,\
                       CASE \
                            WHEN Age < 25 THEN 'Inherited' \
                            WHEN NetWorth > 100000 THEN 'Inherited' \
                            ELSE Null \
                        END) as MarketingNameplate,\
                       CAST('True' as BOOLEAN) as IsCurrent,\
                       CAST('1' as INT) as BatchID,\
                       to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate,\
                       to_date('9999-12-31', 'yyyy-MM-dd') as EndDate\
                       From customers as c \
                       left join TaxRate_broad as NAT on c.C_NAT_TX_ID = NAT.TX_ID\
                       left join TaxRate_broad as LCL on c.C_LCL_TX_ID = LCL.TX_ID\
                       left join Prospect_broad as p on (c.C_L_NAME = p.LastName and c.C_F_NAME = p.FirstName and c.C_ADLINE1 = p.AddressLine1 and c.C_ADLINE2 =  p.AddressLine2 and c.C_ZIPCODE = p.PostalCode)")

```


```python
file_rdd.unpersist()
new_customer_records_rdd.unpersist()
new_customer_df.unpersist()
new_account_df.unpersist()
add_account_records_rdd.unpersist()
add_account_df.unpersist()
Accounts.unpersist()
inactive_accounts.unpersist()
update_customer_rdd.unpersist()
update_customer_df.unpersist()
customers_not_updated.unpersist()
customers_updated.unpersist()
Customers.unpersist()
df_TaxRate.unpersist()
df_Prospect.unpersist()
df_TaxRate_broad.unpersist()
df_Prospect_broad.unpersist()
```




    DataFrame[AgencyID: string, LastName: string, FirstName: string, MiddleInitial: string, Gender: string, AddressLine1: string, AddressLine2: string, PostalCode: string, City: string, State: string, Country: string, Phone: string, Income: int, NumberCars: int, NumberChildren: int, MaritalStatus: string, Age: int, CreditRating: int, OwnOrRentFlag: string, Employer: string, NumberCreditCards: int, NetWorth: int]




```python
inact_func = udf(lambda x: 'Inactive' if str(x) in inact_list else 'Active')

dimCustomer = dimCustomer.withColumn('Status', inact_func(dimCustomer.CustomerID))
dimCustomer.show(10)
dimCustomer.createOrReplaceTempView("tbldimCustomer")
```

    +----------+-----------+--------+---------+-------------+------+----+----------+--------------------+------------+----------+---------------+--------------------+--------------------+------------------+------------+--------------+--------------------+--------------------+--------------------+---------------+--------------------+------------+--------+------------+--------+------------------+---------+-------+-------------+----------+--------+
    |CustomerID|      TaxID|LastName|FirstName|MiddleInitial|Gender|Tier|       DOB|        AddressLine1|AddressLine2|PostalCode|           City|           StateProv|             Country|            Phone1|      Phone2|        Phone3|              Email1|              Email2| NationalTaxRateDesc|NationalTaxRate|    LocalTaxRateDesc|LocalTaxRate|AgencyID|CreditRating|NetWorth|MarketingNameplate|IsCurrent|BatchID|EffectiveDate|   EndDate|  Status|
    +----------+-----------+--------+---------+-------------+------+----+----------+--------------------+------------+----------+---------------+--------------------+--------------------+------------------+------------+--------------+--------------------+--------------------+--------------------+---------------+--------------------+------------+--------+------------+--------+------------------+---------+-------+-------------+----------+--------+
    |     10096|214-25-1030|  Polder|     Kyla|         null|  null|   3|1997-09-15|    15031 Wood South|        null|     29409|          Omaha|                  CA|United States of ...|      533 560-6464|950 140-4160|1 217 452-4683|  Kyla.Polder@gmx.hk|                null|Arkansas Tax for ...|         0.0567|Oklahoma Income T...|      0.0318|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |     10351|461-07-0288|   Perez| Germaine|            J|  null|   3|1981-04-02|    24160 Star Drive|        null|     66624| San Bernardino|Prince Edward Island|United States of ...|          084-5037|        null|          null|Germaine.J.Perez@...|                null|Montana Income Ta...|            0.1|Iowa Income Tax f...|      0.0467|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |      1090|668-12-9960| Grisoni|    Steve|         null|  null|   2|2011-12-08|22820 Wilshire Bo...|        null|     33108|Fort Lauderdale|                  NC|United States of ...|          200-8103|530 132-0772|          null|Steve.Grisoni@sht...|Steve.Grisoni@gmx.sg|Wyoming Income Ta...|            0.0|Montana Income Ta...|        0.04|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|Inactive|
    |     11332|460-09-5021|  Cawley|   Dulcie|            K|     f|   3|1974-03-16|4953 Williams Bou...|        null|     93792| Corpus Christi|                  SC|United States of ...|421 969-8302 70924|        null|1 324 726-9321|Dulcie.K.Cawley@p...|Dulcie.K.Cawley@l...|Missouri Income T...|           0.04|California Tax fo...|      0.0432|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |     11563|972-84-0885|   Adcox|      Viv|         null|     M|   3|1982-07-02|        3020 Fox Rue|        null|     33613|          Sandy|                  OK|United States of ...|    1 536 315-5937|        null|          null|Viv.Adcox@soodoni...|Viv.Adcox@letterb...|California Tax fo...|         0.0432|Northwest Territo...|       0.434|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |      1159|502-65-7980|  Widdis|   Jennee|            S|  null|   2|2017-02-19|    15031 Wood South|        null|     53714|        Madison|                  HI|United States of ...|      575 525-5498|760 164-1499|1 640 959-7333|Jennee.S.Widdis@s...|                null|North Dakota Inco...|         0.0667|District of Colum...|       0.095|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |     11722|580-43-0559|  Senese|  Kristen|            F|  null|null|1986-05-18|19570 Winter Park...|    Apt. 602|     73132|       Victoria|            Manitoba|United States of ...|589 643-8343 21134|        null|          null|Kristen.F.Senese@...|Kristen.F.Senese@...|Utah Income Tax f...|           0.07|Nova Scotia Incom...|      0.3077|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |     11888|774-76-4757|   Mader|   Ronnie|         null|  null|   3|1935-05-15|      5317 Drew East|     Apt. 34|     72909|     Scottsdale|                  VT|United States of ...|      992 459-1649|    836-3228|          null|Ronnie.Mader@hush...|                null|Saskatchewan Inco...|           0.38|Iowa Income Tax f...|      0.0252|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    |     12394|258-56-5579|    Nass|   Mehmud|         null|  null|   3|1958-12-10| 22083 Crossus Lower|        null|   M5B 1Y5|         Pomona|                  VA|              Canada|          359-0824|        null|          null|Mehmud.Nass@kify.net|                null|Idaho Income Tax ...|         0.0466|Illinois Income T...|        0.03|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|Inactive|
    |     12529|662-49-8573| Szamosi|   Pearla|            A|  null|   1|1941-03-27|   10314 Presido Way|        null|     77126|         Nashua|                  AR|              Canada|    695-6853 06892|809 123-6709|          null|Pearla.A.Szamosi@...|                null|New Brunswick Inc...|           0.16|Kentucky Income T...|        0.03|    null|        null|    null|              null|     true|      1|   2015-01-01|9999-12-31|  Active|
    +----------+-----------+--------+---------+-------------+------+----+----------+--------------------+------------+----------+---------------+--------------------+--------------------+------------------+------------+--------------+--------------------+--------------------+--------------------+---------------+--------------------+------------+--------+------------+--------+------------------+---------+-------+-------------+----------+--------+
    only showing top 10 rows
    



```python
schema_Date = StructType([
    StructField("SK_DateID", IntegerType(), False),
    StructField("DateValue", StringType(), False),
    StructField("DateDesc", StringType(), False),
    StructField("CalendarYearID", IntegerType(), False),
    StructField("CalendarYearDesc", StringType(), False),
    StructField("CalendarQtrID", IntegerType(), False),
    StructField("CalendarQtrDesc", StringType(), False),
    
    StructField("CalendarMonthID", IntegerType(), False),
    StructField("CalendarMonthDesc", StringType(), False),
    StructField("CalendarWeekID", IntegerType(), False),
    StructField("CalendarWeekDesc", StringType(), False),
    
    StructField("DayOfWeekNum", IntegerType(), False),
    StructField("DayOfWeekDesc", StringType(), False),
    StructField("FiscalYearID", IntegerType(), False),
    StructField("FiscalYearDesc", StringType(), False),
    
    StructField("FiscalQtrID", IntegerType(), False),
    StructField("FiscalQtrDesc", StringType(), False),
    
    StructField("HolidayFlag", BooleanType(), True)])
```


```python
df_Date = spark.read\
            .format("csv")\
            .schema(schema_Date)\
            .option("header", "false")\
            .option("sep", "|")\
            .load("./Dataset/Date.txt")
```


```python
df_Date.show(10)
```

    +---------+----------+----------------+--------------+----------------+-------------+---------------+---------------+-----------------+--------------+----------------+------------+-------------+------------+--------------+-----------+-------------+-----------+
    |SK_DateID| DateValue|        DateDesc|CalendarYearID|CalendarYearDesc|CalendarQtrID|CalendarQtrDesc|CalendarMonthID|CalendarMonthDesc|CalendarWeekID|CalendarWeekDesc|DayOfWeekNum|DayOfWeekDesc|FiscalYearID|FiscalYearDesc|FiscalQtrID|FiscalQtrDesc|HolidayFlag|
    +---------+----------+----------------+--------------+----------------+-------------+---------------+---------------+-----------------+--------------+----------------+------------+-------------+------------+--------------+-----------+-------------+-----------+
    | 19500101|1950-01-01| January 1, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           7|       Sunday|        1950|          1950|      19503|      1950 Q3|       true|
    | 19500102|1950-01-02| January 2, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           1|       Monday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500103|1950-01-03| January 3, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           2|      Tuesday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500104|1950-01-04| January 4, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           3|    Wednesday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500105|1950-01-05| January 5, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           4|     Thursday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500106|1950-01-06| January 6, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           5|       Friday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500107|1950-01-07| January 7, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           6|     Saturday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500108|1950-01-08| January 8, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19502|         1950-W2|           7|       Sunday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500109|1950-01-09| January 9, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19502|         1950-W2|           1|       Monday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500110|1950-01-10|January 10, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19502|         1950-W2|           2|      Tuesday|        1950|          1950|      19503|      1950 Q3|      false|
    +---------+----------+----------------+--------------+----------------+-------------+---------------+---------------+-----------------+--------------+----------------+------------+-------------+------------+--------------+-----------+-------------+-----------+
    only showing top 10 rows
    



```python
df_Date.printSchema()
```

    root
     |-- SK_DateID: integer (nullable = true)
     |-- DateValue: string (nullable = true)
     |-- DateDesc: string (nullable = true)
     |-- CalendarYearID: integer (nullable = true)
     |-- CalendarYearDesc: string (nullable = true)
     |-- CalendarQtrID: integer (nullable = true)
     |-- CalendarQtrDesc: string (nullable = true)
     |-- CalendarMonthID: integer (nullable = true)
     |-- CalendarMonthDesc: string (nullable = true)
     |-- CalendarWeekID: integer (nullable = true)
     |-- CalendarWeekDesc: string (nullable = true)
     |-- DayOfWeekNum: integer (nullable = true)
     |-- DayOfWeekDesc: string (nullable = true)
     |-- FiscalYearID: integer (nullable = true)
     |-- FiscalYearDesc: string (nullable = true)
     |-- FiscalQtrID: integer (nullable = true)
     |-- FiscalQtrDesc: string (nullable = true)
     |-- HolidayFlag: boolean (nullable = true)
    



```python
df_Date.createOrReplaceTempView("date")
```


```python
dimDate = spark.sql("\
                       Select SK_DateID,\
                           Cast(DateValue as date),\
                           DateDesc,\
                           CalendarYearID,\
                           CalendarYearDesc,\
                           CalendarQtrID,\
                           CalendarQtrDesc,\
                           CalendarMonthID,\
                           CalendarMonthDesc,\
                           CalendarWeekID,\
                           CalendarWeekDesc,\
                           DayOfWeekNum,\
                           DayOfWeekDesc,\
                           FiscalYearID,\
                           FiscalYearDesc,\
                           FiscalQtrID,\
                           FiscalQtrDesc,\
                           HolidayFlag\
                       From date")
```


```python
dimDate.show(10)
```

    +---------+----------+----------------+--------------+----------------+-------------+---------------+---------------+-----------------+--------------+----------------+------------+-------------+------------+--------------+-----------+-------------+-----------+
    |SK_DateID| DateValue|        DateDesc|CalendarYearID|CalendarYearDesc|CalendarQtrID|CalendarQtrDesc|CalendarMonthID|CalendarMonthDesc|CalendarWeekID|CalendarWeekDesc|DayOfWeekNum|DayOfWeekDesc|FiscalYearID|FiscalYearDesc|FiscalQtrID|FiscalQtrDesc|HolidayFlag|
    +---------+----------+----------------+--------------+----------------+-------------+---------------+---------------+-----------------+--------------+----------------+------------+-------------+------------+--------------+-----------+-------------+-----------+
    | 19500101|1950-01-01| January 1, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           7|       Sunday|        1950|          1950|      19503|      1950 Q3|       true|
    | 19500102|1950-01-02| January 2, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           1|       Monday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500103|1950-01-03| January 3, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           2|      Tuesday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500104|1950-01-04| January 4, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           3|    Wednesday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500105|1950-01-05| January 5, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           4|     Thursday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500106|1950-01-06| January 6, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           5|       Friday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500107|1950-01-07| January 7, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19501|         1950-W1|           6|     Saturday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500108|1950-01-08| January 8, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19502|         1950-W2|           7|       Sunday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500109|1950-01-09| January 9, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19502|         1950-W2|           1|       Monday|        1950|          1950|      19503|      1950 Q3|      false|
    | 19500110|1950-01-10|January 10, 1950|          1950|            1950|        19501|        1950 Q1|          19501|     1950 January|         19502|         1950-W2|           2|      Tuesday|        1950|          1950|      19503|      1950 Q3|      false|
    +---------+----------+----------------+--------------+----------------+-------------+---------------+---------------+-----------------+--------------+----------------+------------+-------------+------------+--------------+-----------+-------------+-----------+
    only showing top 10 rows
    



```python
dimDate.printSchema()
```

    root
     |-- SK_DateID: integer (nullable = true)
     |-- DateValue: date (nullable = true)
     |-- DateDesc: string (nullable = true)
     |-- CalendarYearID: integer (nullable = true)
     |-- CalendarYearDesc: string (nullable = true)
     |-- CalendarQtrID: integer (nullable = true)
     |-- CalendarQtrDesc: string (nullable = true)
     |-- CalendarMonthID: integer (nullable = true)
     |-- CalendarMonthDesc: string (nullable = true)
     |-- CalendarWeekID: integer (nullable = true)
     |-- CalendarWeekDesc: string (nullable = true)
     |-- DayOfWeekNum: integer (nullable = true)
     |-- DayOfWeekDesc: string (nullable = true)
     |-- FiscalYearID: integer (nullable = true)
     |-- FiscalYearDesc: string (nullable = true)
     |-- FiscalQtrID: integer (nullable = true)
     |-- FiscalQtrDesc: string (nullable = true)
     |-- HolidayFlag: boolean (nullable = true)
    



```python
schema_CashTrans = StructType([
    StructField("CDC_FLAG", StringType(), False),
    StructField("CDC_DSN", IntegerType(), False),
    StructField("CT_CA_ID", IntegerType(), False),
    StructField("CT_DTS", TimestampType(), False),
    StructField("CT_AMT", FloatType(), False),
    StructField("CT_NAME", StringType(), False)])
```


```python
df_CashTrans = spark.read\
            .format("csv")\
            .schema(schema_CashTrans)\
            .option("header", "false")\
            .option("sep", "|")\
            .load("./Dataset/Batch2/CashTransaction.txt")
```


```python
df_CashTrans.show(10)
```

    +--------+-------+--------+-------------------+----------+--------------------+
    |CDC_FLAG|CDC_DSN|CT_CA_ID|             CT_DTS|    CT_AMT|             CT_NAME|
    +--------+-------+--------+-------------------+----------+--------------------+
    |       I|4937695|    6507|2017-07-08 10:16:09|   5519.45|AYJRCJpzLBMJUWKjS...|
    |       I|4938687|    1571|2017-07-08 15:07:22| 623871.06|jPmEvxgxaeaq Uxqu...|
    |       I|4938975|   11708|2017-07-08 17:55:33|   8712.61|BIOgCYoEPlRuRUiMG...|
    |       I|4940035|   12403|2017-07-08 22:55:52|-516203.06|          GO ERHHSVO|
    |       I|4941475|    4189|2017-07-08 21:37:17|  -3084.95|FIgutPGqjffXeBwvY...|
    |       I|4941627|    4212|2017-07-08 04:46:55|-216073.48|mTyTAMFpoVUAuNfxs...|
    |       I|4942163|    6390|2017-07-08 14:43:48| -58952.78|GAuMSSixIeaqGjZZL...|
    |       I|4942839|    9836|2017-07-08 19:28:33|   4761.51|         sqTyZKkSGVG|
    |       I|4943923|    5689|2017-07-08 05:35:45|  -3700.86|fdMCOdoGxgfVCMpgu...|
    |       I|4944143|    5689|2017-07-08 07:08:58|   8300.37|RfQEMlybsWPp YfrGzVm|
    +--------+-------+--------+-------------------+----------+--------------------+
    only showing top 10 rows
    



```python
df_CashTrans.createOrReplaceTempView("cashTrans")
dimDate.createOrReplaceTempView("dimDate_tbl")
dimAccount.createOrReplaceTempView("dimAccount_tbl")
```


```python
factCashBalances = spark.sql("\
                       Select CustomerID,\
                           AccountID,\
                           SK_DateID,\
                           sum(CT_AMT) as Cash,\
                           CAST('1' as INT) as BatchID\
                       From cashTrans join dimAccount_tbl as ac on (CT_CA_ID =ac.AccountID)\
                       join dimDate_tbl as dt on dt.DateValue = Date(CT_DTS)\
                       Group by AccountID, CustomerID, SK_DateID")
```


```python
factCashBalances.show(5)
```

    +----------+---------+---------+------------------+-------+
    |CustomerID|AccountID|SK_DateID|              Cash|BatchID|
    +----------+---------+---------+------------------+-------+
    |      1337|     2562| 20170708|    -87205.7421875|      1|
    |      1514|     9587| 20170708|1647.3900146484375|      1|
    |       383|    11259| 20170708|-2605.860107421875|      1|
    |      4858|     9618| 20170708| -3108.56005859375|      1|
    |      1356|     3627| 20170708|   8159.0498046875|      1|
    +----------+---------+---------+------------------+-------+
    only showing top 5 rows
    



```python
dimAccount.write.parquet("dimAccount.parquet")
```


```python
dimDate.write.parquet("dimDate.parquet")
```


```python
dimCustomer.write.parquet("dimCustomer.parquet")
```


```python
factCashBalances.write.parquet("factCashBalances.parquet")
```
