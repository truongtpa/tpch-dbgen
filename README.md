### Generate TPC-H

```
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make
./dbgen -s 10

mkdir tpch-10
mv *.tbl ./tpch-10
```

### Convert to Parquet
```
mkdir tpch-10-parquet
pip install pandas pyarrow
python3 to2parquet.py /tmp/tpch-dbgen/tpch-10/ /tmp/tpch-dbgen/tpch-10/
```

### To S3
```
https://dl.min.io/client/mc/release/linux-amd64/mc

```
