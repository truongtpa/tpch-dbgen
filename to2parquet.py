#!/usr/bin/env python3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from decimal import Decimal
import sys

SCHEMAS = {
    "customer": pa.schema([
        ("c_custkey", pa.int32()),
        ("c_name", pa.string()),
        ("c_address", pa.string()),
        ("c_nationkey", pa.int32()),
        ("c_phone", pa.string()),
        ("c_acctbal", pa.decimal128(15, 2)),
        ("c_mktsegment", pa.string()),
        ("c_comment", pa.string()),
    ]),

    "orders": pa.schema([
        ("o_orderkey", pa.int32()),
        ("o_custkey", pa.int32()),
        ("o_orderstatus", pa.string()),
        ("o_totalprice", pa.decimal128(15, 2)),
        ("o_orderdate", pa.date32()),
        ("o_orderpriority", pa.string()),
        ("o_clerk", pa.string()),
        ("o_shippriority", pa.int32()),
        ("o_comment", pa.string()),
    ]),

    "lineitem": pa.schema([
        ("l_orderkey", pa.int32()),
        ("l_partkey", pa.int32()),
        ("l_suppkey", pa.int32()),
        ("l_linenumber", pa.int32()),
        ("l_quantity", pa.decimal128(15, 2)),
        ("l_extendedprice", pa.decimal128(15, 2)),
        ("l_discount", pa.decimal128(15, 2)),
        ("l_tax", pa.decimal128(15, 2)),
        ("l_returnflag", pa.string()),
        ("l_linestatus", pa.string()),
        ("l_shipdate", pa.date32()),
        ("l_commitdate", pa.date32()),
        ("l_receiptdate", pa.date32()),
        ("l_shipinstruct", pa.string()),
        ("l_shipmode", pa.string()),
        ("l_comment", pa.string()),
    ]),

    "part": pa.schema([
        ("p_partkey", pa.int32()),
        ("p_name", pa.string()),
        ("p_mfgr", pa.string()),
        ("p_brand", pa.string()),
        ("p_type", pa.string()),
        ("p_size", pa.int32()),
        ("p_container", pa.string()),
        ("p_retailprice", pa.decimal128(15, 2)),
        ("p_comment", pa.string()),
    ]),

    "partsupp": pa.schema([
        ("ps_partkey", pa.int32()),
        ("ps_suppkey", pa.int32()),
        ("ps_availqty", pa.int32()),
        ("ps_supplycost", pa.decimal128(15, 2)),
        ("ps_comment", pa.string()),
    ]),

    "supplier": pa.schema([
        ("s_suppkey", pa.int32()),
        ("s_name", pa.string()),
        ("s_address", pa.string()),
        ("s_nationkey", pa.int32()),
        ("s_phone", pa.string()),
        ("s_acctbal", pa.decimal128(15, 2)),
        ("s_comment", pa.string()),
    ]),

    "nation": pa.schema([
        ("n_nationkey", pa.int32()),
        ("n_name", pa.string()),
        ("n_regionkey", pa.int32()),
        ("n_comment", pa.string()),
    ]),

    "region": pa.schema([
        ("r_regionkey", pa.int32()),
        ("r_name", pa.string()),
        ("r_comment", pa.string()),
    ]),
}


def convert_table(input_dir, output_dir, table):
    schema = SCHEMAS[table]
    cols = [f.name for f in schema]

    print(f"Converting {table}...")

    # Đọc tất cả dưới dạng string
    df = pd.read_csv(
        os.path.join(input_dir, f"{table}.tbl"),
        sep="|",
        header=None,
        names=cols + ["_extra"],
        usecols=cols,
        dtype=str
    )

    # Cast DATE
    for col in cols:
        if col.endswith("date"):
            df[col] = pd.to_datetime(df[col], errors="raise")

    # Cast INT
    for field in schema:
        if pa.types.is_integer(field.type):
            df[field.name] = df[field.name].astype("int64")

    # Cast DECIMAL → decimal.Decimal (CỰC KỲ QUAN TRỌNG)
    for field in schema:
        if pa.types.is_decimal(field.type):
            df[field.name] = df[field.name].apply(
                lambda x: None if x is None or x == "" else Decimal(x)
            )

    table_arrow = pa.Table.from_pandas(
        df,
        schema=schema,
        preserve_index=False
    )

    pq.write_table(
        table_arrow,
        os.path.join(output_dir, f"{table}.parquet"),
        compression="snappy",
        row_group_size=100_000
    )

    print(f"✓ {table}: {len(df)} rows")

def convert_table_stream(input_dir, output_dir, table, chunksize=1_000_000):
    schema = SCHEMAS[table]
    cols = [f.name for f in schema]

    print(f"Converting {table} (streaming)...")

    writer = None
    total_rows = 0
    out_path = os.path.join(output_dir, f"{table}.parquet")

    for chunk in pd.read_csv(
            os.path.join(input_dir, f"{table}.tbl"),
            sep="|",
            header=None,
            names=cols + ["_extra"],
            usecols=cols,
            dtype=str,
            chunksize=chunksize
    ):
        # DATE
        for col in cols:
            if col.endswith("date"):
                chunk[col] = pd.to_datetime(chunk[col], errors="raise")

        # INT
        for field in schema:
            if pa.types.is_integer(field.type):
                chunk[field.name] = chunk[field.name].astype("int64")

        # DECIMAL
        for field in schema:
            if pa.types.is_decimal(field.type):
                chunk[field.name] = chunk[field.name].apply(
                    lambda x: None if x == "" else Decimal(x)
                )

        table_arrow = pa.Table.from_pandas(
            chunk,
            schema=schema,
            preserve_index=False
        )

        if writer is None:
            writer = pq.ParquetWriter(
                out_path,
                schema,
                compression="snappy",
                use_dictionary=True
            )

        writer.write_table(table_arrow)
        total_rows += len(chunk)

        print(f"  + {total_rows:,} rows")

    if writer:
        writer.close()

    print(f"DONE {table}: {total_rows:,} rows")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: to2parquet.py <tpch_tbl_dir> <output_parquet_dir>")
        sys.exit(1)

    in_dir, out_dir = sys.argv[1:3]
    os.makedirs(out_dir, exist_ok=True)

    for table in SCHEMAS:
        convert_table_stream(
            in_dir,
            out_dir,
            table,
            chunksize=5_000_000
        )
