import pandas as pd
import pymysql
from sqlalchemy import create_engine

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xjtuse',
    'database': 'qs',
    'port': 3306
}

def read_excel_file(file_path):
    df = pd.read_excel(file_path)
    return df

def insert_to_database(df):
    if df is None or df.empty:
        return False

    engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
    )

    df.to_sql(
        name='university_rank',
        con=engine,
        if_exists='replace',
            # 'fail'：若表存在则报错。
            # 'replace'：删除原表，重新创建并写入数据。
            # 'append'：在原表后追加数据（常用批量导入场景）。
        index=False,
        chunksize=150
    )
    return True

def main():
    excel_file = "QStop大学排名.xlsx"
    df = read_excel_file(excel_file)
    if df is not None:
        insert_to_database(df)

if __name__ == "__main__":
    main()
