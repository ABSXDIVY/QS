import requests
import json
import pandas as pd
import time
from sqlalchemy import create_engine, text
from datetime import date, datetime
import logging
import os
import traceback
import urllib3

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xjtuse',
    'database': 'qs',
    'port': 3306,
    'charset': 'utf8mb4'
}

# 设置日志
def setup_logging():
    """设置日志配置"""
    # 创建logs目录
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.FileHandler(f'logs/qs_crawler_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8')
        ]
    )
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    
    return logger

logger = setup_logging()

def log_error_notification(error, context=""):
    """记录错误到日志文件"""
    logger.error(f"错误: {context} - {error}")
    logger.error(f"堆栈: {traceback.format_exc()}")

def create_table_if_not_exists(engine):
    """创建数据表"""
    drop_sql = "DROP TABLE IF EXISTS university_rank_simple"
    create_sql = """
    CREATE TABLE university_rank_simple (
        id INT AUTO_INCREMENT PRIMARY KEY,
        `rank` INT NOT NULL,
        overall_score DECIMAL(10, 2),
        university_name VARCHAR(255) NOT NULL,
        country VARCHAR(100),
        city VARCHAR(100),
        crawl_date DATE NOT NULL,
        is_deleted TINYINT(1) NOT NULL DEFAULT 0,
        create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_university_date (university_name, crawl_date),
        INDEX idx_rank (`rank`),
        INDEX idx_crawl_date (crawl_date),
        INDEX idx_country (country),
        INDEX idx_is_deleted (is_deleted)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(drop_sql))
            conn.execute(text(create_sql))
            conn.commit()
    except Exception as e:
        logger.error(f"创建数据表失败: {e}")
        log_error_notification(e, "创建数据表失败")

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.topuniversities.com/world-university-rankings/2025',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache'
})
session.verify = False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

params = {
    'nid': '3990755', 'page': 0, 'items_per_page': 30, 'tab': 'indicators',
    'region': '', 'countries': '', 'cities': '', 'search': '', 'star': '',
    'sort_by': '', 'order_by': '', 'program_type': '', 'scholarship': '',
    'fee': '', 'english_score': '', 'academic_score': '', 'mix_student': '',
    'loggedincache': '6905039-1754356589358'
}

all_universities = []
page = 0
max_pages = 5

try:
    while page < max_pages:
        params['page'] = page
        
        try:
            response = session.get("https://www.topuniversities.com/rankings/endpoint", params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'score_nodes' in data:
                    for uni in data['score_nodes']:
                        try:
                            all_universities.append({
                                'rank': int(uni.get('rank', 0)),
                                'overall_score': float(uni.get('overall_score', 0)),
                                'university_name': uni.get('title', ''),
                                'country': uni.get('country', ''),
                                'city': uni.get('city', '')
                            })
                        except Exception as e:
                            continue
                    
                    if page + 1 >= data.get('total_pages', 1):
                        break
                else:
                    logger.warning("数据格式异常")
                    break
            else:
                logger.error(f"请求失败: {response.status_code}")
                break
        except requests.exceptions.Timeout:
            logger.error("请求超时，重试中...")
            time.sleep(2)
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"网络异常: {str(e)[:100]}...")
            time.sleep(3)
            continue
        except json.JSONDecodeError as e:
            logger.error(f"数据解析失败: {e}")
            break
        
        page += 1
        time.sleep(1)
    
    logger.info(f"爬取完成: {len(all_universities)} 条数据")

except Exception as e:
    logger.error(f"爬取失败: {e}")
    log_error_notification(e, "爬取数据失败")

if all_universities:
    df = pd.DataFrame(all_universities)
    current_date = date.today()
    
    try:
        excel_filename = f'QS大学排名{current_date}.xlsx'
        df.to_excel(excel_filename, index=False)
        logger.info(f"Excel: {excel_filename}")
    except Exception as e:
        logger.error(f"Excel保存失败: {e}")
        log_error_notification(e, "保存Excel文件失败")
    
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
            f"{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
        )
        
        create_table_if_not_exists(engine)
        
        with engine.connect() as conn:
            for _, row in df.iterrows():
                sql = """
                INSERT INTO university_rank_simple 
                (`rank`, overall_score, university_name, country, city, crawl_date)
                VALUES (:rank, :overall_score, :university_name, :country, :city, :crawl_date)
                ON DUPLICATE KEY UPDATE
                `rank` = VALUES(`rank`),
                overall_score = VALUES(overall_score),
                country = VALUES(country),
                city = VALUES(city)
                """
                conn.execute(text(sql), {
                    'rank': row['rank'],
                    'overall_score': row['overall_score'],
                    'university_name': row['university_name'],
                    'country': row['country'],
                    'city': row['city'],
                    'crawl_date': current_date
                })
            conn.commit()
        
        logger.info(f"数据库: {len(df)} 条记录")
        
    except Exception as e:
        logger.error(f"数据库保存失败: {e}")
        log_error_notification(e, "保存到数据库失败")
else:
    logger.warning("无数据可保存")

session.close()
logger.info("-" * 50)

# 终端反馈完成信息
if all_universities:
    print(f"完成: {len(all_universities)} 条数据")
else:
    print("爬取失败") 