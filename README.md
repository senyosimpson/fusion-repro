# fusion-repro

This is a reproduction of an error when querying Parquet files written in [parquet-go](https://github.com/parquet-go/parquet-go).

For a reproduction case, I generated a very simplistic Parquet file using the `parquet-go` library.
The program is under the directory `go-parquet-writer`.
To save you the hassle of running it, the output of that program is saved under `go-parquet-writer/go-testfile.parquet`.

Every tool I've used to view their metadata makes them look like they are correctly formed (including `datafusion-cli`'s `describe` function (seen below)).

```
» datafusion-cli --command "describe 'go-parquet-writer/go-testfile.parquet'"
DataFusion CLI v44.0.0
+---------------+-------------------------------------+-------------+
| column_name   | data_type                           | is_nullable |
+---------------+-------------------------------------+-------------+
| city          | Utf8View                            | NO          |
| country       | Utf8View                            | NO          |
| age           | UInt8                               | NO          |
| scale         | Int16                               | NO          |
| status        | UInt32                              | NO          |
| time_captured | Timestamp(Millisecond, Some("UTC")) | NO          |
| checked       | Boolean                             | NO          |
+---------------+-------------------------------------+-------------+
7 row(s) fetched.
Elapsed 0.001 seconds.
```

When I run a query over the file with no predicate, it works fine

```
» datafusion-cli --command "select * from 'go-parquet-writer/go-testfile.parquet'"
DataFusion CLI v44.0.0
+--------+---------+-----+-------+--------+--------------------------+---------+
| city   | country | age | scale | status | time_captured            | checked |
+--------+---------+-----+-------+--------+--------------------------+---------+
| Madrid | Spain   | 10  | -1    | 12     | 2025-01-24T16:34:00.715Z | false   |
| Athens | Greece  | 32  | 1     | 20     | 2025-01-24T17:34:00.715Z | true    |
+--------+---------+-----+-------+--------+--------------------------+---------+
2 row(s) fetched.
Elapsed 0.002 seconds.
```

When I run the query with a predicate, it says I have bad data

```
» datafusion-cli --command "select * from 'go-parquet-writer/go-testfile.parquet' where age > 10"
DataFusion CLI v44.0.0
Error: External error: Parquet error: External: bad data
```

I initially ran into this error using datafusion in an application.
It gives a more descriptive error about converting types: `Error: ParquetError(External(ProtocolError { kind: InvalidData, message: "cannot convert 2 into TType" }))`

Using PyArrow, it also works fine.
I made a simple script that loads the Parquet file as a Pandas DataFrame and filters it.
It's found under `pyarrow-ex`, just run `python3 example.py` (needs PyArrow and Pandas dependencies).

```python
def main():
    table = pq.read_table('../go-parquet-writer/go-testfile.parquet')
    df = table.to_pandas()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        print(df[df['age'] > 10])
```
with resulting output

```
» python3 example.py
     city country  age  scale  status                    time_captured  checked
1  Athens  Greece   32      1      20 2025-01-24 17:34:00.715000+00:00     True
```

## Debugging

From everything I've gathered, this error is likely coming from this [conversion function](https://github.com/apache/thrift/blob/7734c393ed0f0632c658c05e33a4d6592cf2912c/lib/rs/src/protocol/compact.rs#L660-L679).
However, it only skips checking `0x02` when a [collection is being parsed](https://github.com/apache/thrift/blob/7734c393ed0f0632c658c05e33a4d6592cf2912c/lib/rs/src/protocol/compact.rs#L653-L658).
The only weird thing is I have no map/list in my schema.
I assume this means this `0x02` is being used to encode something else but it is beyond my knowledge.

I went spelunking in `parquet-go` codebase. The Thrift protocol implementation is split amongst [the compact protocol](https://github.com/parquet-go/parquet-go/blob/main/encoding/thrift/compact.go) and [the Thrift type definitions](https://github.com/parquet-go/parquet-go/blob/main/encoding/thrift/thrift.go) and [the encoding logic](https://github.com/parquet-go/parquet-go/blob/main/encoding/thrift/encode.go)
