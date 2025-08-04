import requests
import pandas as pd
import time
import re
from sqlalchemy import create_engine, text, exc
from datetime import datetime, date

# 数据库配置（保持不变）
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xjtuse',
    'database': 'qs',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}


def check_and_create_table(engine):
    """检查并创建支持历史数据的表结构（保持不变）"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                                       SELECT COUNT(*)
                                       FROM information_schema.tables
                                       WHERE table_schema = :db
                                         AND table_name = 'university_rank'
                                       """), {"db": DB_CONFIG['database']})

            if result.scalar() == 0:
                print("创建支持历史数据的表 university_rank...")
                conn.execute(text("""
                                  CREATE TABLE university_rank
                                  (
                                      id              INT AUTO_INCREMENT PRIMARY KEY,
                                      university_name VARCHAR(255) NOT NULL,
                                      `rank`          INT          NOT NULL,
                                      overall_score   DECIMAL(10, 2),
                                      location        VARCHAR(255),
                                      crawl_date      DATE         NOT NULL,
                                      create_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                      INDEX idx_crawl_date (crawl_date),
                                      INDEX idx_university (university_name),
                                      INDEX idx_rank (`rank`)
                                  ) ENGINE = InnoDB
                                    DEFAULT CHARSET = :charset
                                    COLLATE = :collation;
                                  """), {
                                 "charset": DB_CONFIG['charset'],
                                 "collation": DB_CONFIG['collation']
                             })
                conn.commit()
                print("表创建成功")
            else:
                print("表已存在，确认结构兼容性")
    except exc.SQLAlchemyError as e:
        print(f"表操作失败: {str(e)}")
        raise


def init_database():
    """初始化数据库连接（保持不变）"""
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
            f"{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
        )
        with engine.connect():
            print("数据库连接成功")
        check_and_create_table(engine)
        return engine
    except exc.SQLAlchemyError as e:
        print(f"数据库初始化失败: {str(e)}")
        return None


def extract_data(api_response):
    page_data = []
    try:
        universities = api_response.get('data', [])

        for item in universities:
            try:
                rank_text = item.get('rank', '')
                rank_match = re.search(r'\d+', rank_text)
                if not rank_match:
                    print(f"无法提取排名数字: {rank_text}")
                    continue
                rank = int(rank_match.group())

                # 解析总分
                overall_score = item.get('overall_score', '')
                overall_score = re.sub(r'[^\d.]', '', str(overall_score))

                # 解析大学名称和地点
                university_name = item.get('university', '').strip()
                location = item.get('location', '').strip()
                location = re.sub(r'\s+', ' ', location).replace("location", "").strip()

                page_data.append({
                    'rank': rank,
                    'overall_score': float(overall_score) if overall_score else None,
                    'university_name': university_name,
                    'location': location
                })
            except Exception as e:
                print(f"单条数据提取失败: {e}")
                continue
        print(f"成功提取 {len(page_data)} 条数据")
    except Exception as e:
        print(f"数据提取失败: {e}")
    return pd.DataFrame(page_data)


def check_existing_data(engine, crawl_date):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM university_rank WHERE crawl_date = :date"),
                {"date": crawl_date}
            )
            return result.scalar() > 0
    except exc.SQLAlchemyError as e:
        print(f"检查已有数据失败: {str(e)}")
        return False


def insert_to_database(df, engine, crawl_date):
    if df is None or df.empty or engine is None:
        print("无数据可写入")
        return False

    df['crawl_date'] = crawl_date

    temp_table = "temp_university_rank"
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()

        print(f"创建临时表 {temp_table}...")
        create_temp_sql = f"""
            CREATE TABLE IF NOT EXISTS {temp_table} (
                university_name VARCHAR(255) NOT NULL,
                `rank` INT NOT NULL,
                overall_score DECIMAL(10, 2),
                location VARCHAR(255),
                crawl_date DATE NOT NULL
            ) ENGINE = InnoDB
              DEFAULT CHARSET = :charset
              COLLATE = :collation;
        """
        conn.execute(text(create_temp_sql), {
            "charset": DB_CONFIG['charset'],
            "collation": DB_CONFIG['collation']
        })
        conn.execute(text(f"TRUNCATE TABLE {temp_table}"))

        df.to_sql(
            name=temp_table,
            con=conn,
            if_exists='append',
            index=False
        )

        temp_count = conn.execute(text(f"SELECT COUNT(*) FROM {temp_table}")).scalar()
        if temp_count == 0:
            print("临时表无数据，终止写入")
            trans.rollback()
            return False
        print(f"临时表 {temp_table} 写入 {temp_count} 条数据")

        print("开始向正式表插入数据...")
        insert_sql = f"""
            INSERT INTO university_rank 
            (university_name, `rank`, overall_score, location, crawl_date)
            SELECT university_name, `rank`, overall_score, location, crawl_date
            FROM {temp_table}
        """
        result = conn.execute(text(insert_sql))
        trans.commit()
        print(f"正式表插入成功，新增 {result.rowcount} 条记录")
        return True
    except exc.SQLAlchemyError as e:
        print(f"数据库写入失败: {str(e)}")
        if conn:
            trans.rollback()
        return False
    finally:
        if conn:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
                print(f"临时表 {temp_table} 已删除")
            except exc.SQLAlchemyError as e:
                print(f"删除临时表失败: {str(e)}")
            finally:
                conn.close()


def scrape_and_save_to_db():
    engine = init_database()
    if not engine:
        return

    today = date.today()
    crawl_date = today.replace(day=1)  # 本月1号
    print(f"本次爬取日期: {crawl_date}")

    if check_existing_data(engine, crawl_date):
        print(f"该日期({crawl_date})已存在数据，无需重复爬取")
        return

    try:
        # 模拟浏览器请求头（关键：避免被识别为爬虫）
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'Referer': 'https://www.topuniversities.com/world-university-rankings/2025?items_per_page=150',
            'Accept': 'application/json, text/plain, */*'
        }

        # 这里的URL和参数是示例，需通过浏览器开发者工具获取真实接口
        url = "https://www.topuniversities.com/world-university-rankings/2025?items_per_page=150"
        params = {
            'region': 'asia',  # 对应原代码的地区筛选
            'limit': 1000,  # 一次获取的最大数量
            'sort_by': 'rank'  # 按排名排序
        }

        # 发送请求获取数据
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        api_data = response.json()  # 解析JSON数据

        # 提取并处理数据
        df = extract_data(api_data)

        # 保存到Excel
        excel_filename = f'QS大学排名_{crawl_date}.xlsx'
        df.to_excel(excel_filename, index=False)
        print(f"数据已保存到 {excel_filename}")

        # 插入数据库
        if not df.empty:
            insert_to_database(df, engine, crawl_date)
        print(f"总处理数据量: {len(df)}")

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
    except Exception as e:
        print(f"爬取过程出错: {e}")


if __name__ == "__main__":
    scrape_and_save_to_db()