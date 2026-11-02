#!/usr/bin/env python3
"""
jobs/benchmark_parquet.py — Дополнительное задание ЛР2:
Сравнение параметров записи Parquet (кодеки uncompressed, snappy, gzip, zstd и ROW_GROUP_SIZE).
"""

import subprocess
import os
import re
import json

CODECS = ["uncompressed", "snappy", "gzip", "zstd"]
ROW_GROUP_SIZES = [2000, 20000, 122880]

BENCH_DIR = "data/duckdb/parquet/test_bench"
CONTAINER_DIR = "/data/duckdb/parquet/test_bench"

QUERY_TEMPLATE = """
.timer on
SELECT 
    strftime(event_time, '%Y-%m-%d %H:00') as hour_slot,
    type,
    count(*) as count,
    count(distinct actor_login) as unique_actors
FROM read_parquet('{file_path}')
GROUP BY 1, 2
ORDER BY 1, 2;
"""

def main():
    os.makedirs(BENCH_DIR, exist_ok=True)
    results = []

    print("=" * 75)
    print(" Сравнение параметров записи Parquet (ЛР2, доп. задание)")
    print("=" * 75)
    print(f"{'Кодек':<14} | {'ROW_GROUP_SIZE':<14} | {'Размер (КБ)':<12} | {'Групп строк':<12} | {'Медиана времени (с)':<18}")
    print("-" * 75)

    for codec in CODECS:
        for rgs in ROW_GROUP_SIZES:
            file_name = f"events_{codec}_{rgs}.parquet"
            local_path = os.path.join(BENCH_DIR, file_name)
            container_path = f"{CONTAINER_DIR}/{file_name}"

            # 1. Запись файла с заданными параметрами
            copy_sql = f"""
            COPY (SELECT * FROM read_parquet('/data/duckdb/parquet/events/event_date=2026-10-10/*.parquet'))
            TO '{container_path}' (FORMAT parquet, CODEC '{codec}', ROW_GROUP_SIZE {rgs});
            """
            subprocess.run(
                ["docker", "compose", "run", "--rm", "-T", "duckdb"],
                input=copy_sql.encode(),
                check=True,
                stdout=subprocess.DEVNULL
            )

            # 2. Размер файла на диске
            fsize_bytes = os.path.getsize(local_path)
            fsize_kb = round(fsize_bytes / 1024, 1)

            # 3. Число групп строк через parquet_metadata()
            meta_sql = f"""
            SELECT count(distinct row_group_id) as rg_count
            FROM parquet_metadata('{container_path}');
            """
            p = subprocess.run(
                ["docker", "compose", "run", "--rm", "-T", "duckdb", "-json"],
                input=meta_sql.encode(),
                capture_output=True,
                check=True
            )
            meta = json.loads(p.stdout.decode())[0]
            rg_count = meta["rg_count"]

            # 4. Замер времени запроса (3 запуска, медиана)
            bench_sql = QUERY_TEMPLATE.format(file_path=container_path)
            times = []
            for _ in range(3):
                p_run = subprocess.run(
                    ["docker", "compose", "run", "--rm", "-T", "duckdb"],
                    input=bench_sql.encode(),
                    capture_output=True
                )
                m = re.search(r"Run Time \(s\): real ([0-9.]+)", p_run.stdout.decode())
                if m:
                    times.append(float(m.group(1)))
            times.sort()
            med_time = times[len(times) // 2] if times else 0.0

            res_entry = {
                "codec": codec,
                "row_group_size": rgs,
                "size_kb": fsize_kb,
                "rg_count": rg_count,
                "time_s": med_time
            }
            results.append(res_entry)
            print(f"{codec:<14} | {rgs:<14} | {fsize_kb:>12} | {rg_count:>12} | {med_time:>18.4f}")

    print("=" * 75)
    return results

if __name__ == "__main__":
    main()

