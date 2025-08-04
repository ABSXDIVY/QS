from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from sqlalchemy import create_engine, text, exc
from datetime import datetime

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xjtuse',
    'database': 'qs',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'  # 统一排序规则
}


def check_and_create_table(engine):
    """检查并创建正式表结构，指定统一的排序规则"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                                       SELECT COUNT(*)
                                       FROM information_schema.tables
                                       WHERE table_schema = :db
                                         AND table_name = 'university_rank'
                                       """), {"db": DB_CONFIG['database']})

            if result.scalar() == 0:
                print("创建正式表 university_rank...")
                conn.execute(text("""
                                  CREATE TABLE university_rank
                                  (
                                      university_name VARCHAR(255) PRIMARY KEY,
                                      `rank`          INT      NOT NULL,
                                      overall_score   DECIMAL(10, 2),
                                      location        VARCHAR(255),
                                      create_time     DATETIME NOT NULL,
                                      update_time     DATETIME NOT NULL,
                                      INDEX idx_rank (`rank`)
                                  ) ENGINE = InnoDB
                                    DEFAULT CHARSET = :charset
                                    COLLATE = :collation;
                                  """), {
                                 "charset": DB_CONFIG['charset'],
                                 "collation": DB_CONFIG['collation']
                             })
                conn.commit()
                print("正式表创建成功")
            else:
                print("检查正式表排序规则...")
                conn.execute(text("""
                                  ALTER TABLE university_rank
                                      CONVERT TO CHARACTER SET :charset
                                          COLLATE :collation;
                                  """), {
                                 "charset": DB_CONFIG['charset'],
                                 "collation": DB_CONFIG['collation']
                             })
                conn.commit()
                print("正式表排序规则已确认")
    except exc.SQLAlchemyError as e:
        print(f"正式表操作失败: {str(e)}")
        raise


def init_database():
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
            f"{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
            f"&collation={DB_CONFIG['collation']}"
        )
        with engine.connect():
            print("数据库连接成功")
        check_and_create_table(engine)
        return engine
    except exc.SQLAlchemyError as e:
        print(f"数据库初始化失败: {str(e)}")
        return None


def extract_data(html_source):
    soup = BeautifulSoup(html_source, 'html.parser')
    page_data = []
    try:
        university_cards = soup.select(".new-ranking-cards.normal-row")
        for card in university_cards:
            try:
                rank_text = card.select_one(".left-div-200 .dark-blue .rank-no").get_text(strip=True)
                rank_match = re.search(r'\d+', rank_text)
                if not rank_match:
                    print(f"无法提取排名数字: {rank_text}")
                    continue
                rank = int(rank_match.group())

                # 处理总分
                overall_score = card.select_one(".left-div-200 .light-blue .rank-score").get_text(strip=True)
                overall_score = re.sub(r'[^\d.]', '', overall_score)

                # 处理大学名称和地址
                university_name = card.select_one(".right-div-200 .uni-link").get_text(strip=True)
                location_tag = card.select_one(".right-div-200 .location")
                location = location_tag.get_text(strip=True) if location_tag else ""
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
        print(f"成功提取 {len(page_data)} 条原始数据（已处理并列排名）")
    except Exception as e:
        print(f"数据提取失败: {e}")
    return pd.DataFrame(page_data)


def upsert_to_database(df, engine):
    if df is None or df.empty or engine is None:
        print("无数据可写入")
        return False

    temp_table = "temp_university_rank"
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        print(f"创建临时表 {temp_table}...")
        create_temp_sql = f"""
                          CREATE TABLE IF NOT EXISTS {temp_table} (
                              university_name VARCHAR(255),
                              `rank` INT NOT NULL,
                              overall_score DECIMAL(10, 2),
                              location VARCHAR(255)
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
            if_exists='append',  # 使用append而不是replace，避免重建表导致排序规则变化
            index=False
        )

        # 检查临时表数据量
        temp_count = conn.execute(text(f"SELECT COUNT(*) FROM {temp_table}")).scalar()
        if temp_count == 0:
            print("临时表无数据，终止写入")
            trans.rollback()
            return False
        print(f"临时表 {temp_table} 写入 {temp_count} 条数据")

        # 2. 执行UPSERT写入正式表
        print("开始向正式表写入数据...")
        # 表名直接拼接到SQL中，不使用参数绑定
        upsert_sql = f"""
                                   INSERT INTO university_rank
                                   (university_name, `rank`, overall_score, location, create_time, update_time)
                                   SELECT university_name,
                                          `rank`,
                                          overall_score,
                                          location,
                                          IFNULL((SELECT create_time
                                                  FROM university_rank
                                                  WHERE university_name = temp.university_name), NOW()),
                                          NOW()
                                   FROM {temp_table} temp
                                   ON DUPLICATE KEY UPDATE `rank`        = temp.`rank`,
                                                           overall_score = temp.overall_score,
                                                           location      = temp.location,
                                                           update_time   = NOW()
                                   """
        result = conn.execute(text(upsert_sql))
        trans.commit()  # 提交事务
        print(f"正式表更新成功，影响行数: {result.rowcount}")
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


def scrape_and_save_to_db(keyword):
    engine = init_database()
    if not engine:
        return

    browser = None
    try:
        service = Service(r'C:\Users\Administrator\miniconda3\Scripts\chromedriver.exe')
        browser = webdriver.Chrome(service=service)
        browser.maximize_window()
        url = 'https://www.topuniversities.com/world-university-rankings/2024'
        browser.get(url)
        browser.find_element(By.XPATH, '//*[@id="it-will-be-fixed-top"]/div[2]/div/button/span[1]').click()
        # browser.find_element(By.XPATH,'//*[@id="nav-searchform"]/div[1]/input').send_keys(keyword)
        browser.find_element(By.XPATH, '//*[@id="filterAccordion"]/div[1]/div/div/div').click()
        browser.find_element(By.XPATH, '//*[@id="filterAccordion"]/div[1]/div/div/div/div[2]/div[3]').click()
        browser.find_element(By.XPATH, '//*[@id="newRankingFilters_newUI"]/div[3]/div[7]/div[1]/div[2]/i').click()
        browser.find_element(By.XPATH,
                             '//*[@id="newRankingFilters_newUI"]/div[3]/div[7]/div[1]/div[2]/div[2]/div[4]').click()
        time.sleep(5)

        # 提取并写入数据
        df = extract_data(browser.page_source)
        df.to_excel('QStop大学排名.xlsx', index=False)
        if not df.empty:
            upsert_to_database(df, engine)
        print(f"总处理数据量: {len(df)}")

    except Exception as e:
        print(f"爬取过程出错: {e}")
    finally:
        if browser:
            browser.quit()

scrape_and_save_to_db('QStop')
