#!/usr/bin/env python3
import datetime as dt
import os
import random
import string
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as exc:
    raise SystemExit("pyarrow is required. Install with: pip3 install pyarrow\n%s" % exc)


def rand_str(n=12):
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/megrez-sample-10000.parquet"
    rows = 10000
    random.seed(0)

    ids = list(range(1, rows + 1))
    values = [random.random() * 1000 for _ in range(rows)]
    flags = [random.choice([True, False]) for _ in range(rows)]
    texts = [rand_str() for _ in range(rows)]
    base = dt.datetime(2024, 1, 1)
    ts = [base + dt.timedelta(seconds=i) for i in range(rows)]

    batch = pa.record_batch(
        [ids, values, flags, texts, ts],
        names=["id", "value", "flag", "text", "ts"],
    )

    table = pa.Table.from_batches([batch])
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pq.write_table(table, out_path)
    print(out_path)


if __name__ == "__main__":
    main()
