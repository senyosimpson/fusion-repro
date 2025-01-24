import pyarrow.parquet as pq
import pandas as pd

def main():
    table = pq.read_table('../go-parquet-writer/go-testfile.parquet')
    df = table.to_pandas()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        print(df[df['age'] > 10])


if __name__ == "__main__":
    main()
