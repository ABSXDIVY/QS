import requests
import pandas as pd
import random
import time
import logging
from datetime import datetime, date
from sqlalchemy import create_engine, text
import urllib3

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'qs_data'
}

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(f'logs/fund_crawler_{date.today()}.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def get_session():
    """创建会话"""
    session = requests.Session()
    session.verify = False
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/json',
        'Origin': 'https://gs.amac.org.cn',
        'Referer': 'https://gs.amac.org.cn/amac-infodisc/res/pof/person/personList.html?userId=1700000000699008',
        'X-Requested-With': 'XMLHttpRequest'
    })
    return session

def convert_timestamp(timestamp):
    """转换时间戳"""
    if timestamp:
        try:
            return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    return None

def crawl_fund_data():
    """爬取基金从业资格数据"""
    session = get_session()
    all_personnel = []
    page = 0
    page_size = 20
    max_records = 1000  # 只爬取1000条数据
    
    logger.info("开始爬取基金从业资格数据")
    
    while len(all_personnel) < max_records:
        try:
            # 请求参数
            params = {
                'rand': f"0.{random.randint(1000000000000000, 9999999999999999)}",
                'page': page,
                'size': page_size
            }
            json_data = {
                'userId': '1700000000699008',
                'page': 1
            }
            
            # 发送请求
            response = session.post(
                "https://gs.amac.org.cn/amac-infodisc/api/pof/person",
                params=params,
                json=json_data,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"请求失败: {response.status_code}")
                break
                
            data = response.json()
            content = data.get('content', [])
            
            if not content:
                logger.info("没有更多数据")
                break
            
            # 处理数据
            for person in content:
                cert_obtain_date = convert_timestamp(person.get('certObtainDate'))
                
                personnel_info = {
                    'name': person.get('userName', ''),
                    'gender': person.get('sex', ''),
                    'cert_code': person.get('certCode', ''),
                    'org_name': person.get('orgName', ''),
                    'cert_name': person.get('certName', ''),
                    'cert_obtain_date': cert_obtain_date,
                    'cert_status_change_times': person.get('certStatusChangeTimes', 0),
                    'credit_record_num': person.get('creditRecordNum', 0),
                    'status_name': person.get('statusName', ''),
                    'education_name': person.get('educationName', ''),
                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                all_personnel.append(personnel_info)
                
                # 达到1000条就停止
                if len(all_personnel) >= max_records:
                    break
            
            logger.info(f"第{page+1}页: {len(content)}条数据")
            page += 1
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            logger.error(f"爬取异常: {e}")
            break
    
    logger.info(f"爬取完成: {len(all_personnel)}条数据")
    return all_personnel

def save_to_excel(data):
    """保存到Excel"""
    if not data:
        return
    
    df = pd.DataFrame(data)
    current_date = date.today()
    filename = f"基金从业资格_{current_date}.xlsx"
    
    df.to_excel(filename, index=False)
    logger.info(f"Excel保存成功: {filename}")

def save_to_database(data):
    """保存到数据库"""
    if not data:
        return
    
    try:
        # 连接数据库
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
        )
        
        # 创建表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fund_personnel (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL COMMENT '姓名',
            gender VARCHAR(10) COMMENT '性别',
            cert_code VARCHAR(100) UNIQUE NOT NULL COMMENT '证书编号',
            org_name VARCHAR(255) COMMENT '机构名称',
            cert_name VARCHAR(255) COMMENT '从业资格类别',
            cert_obtain_date DATETIME COMMENT '证书取得日期',
            cert_status_change_times INT COMMENT '证书状态变更记录',
            credit_record_num INT COMMENT '诚信记录',
            status_name VARCHAR(100) COMMENT '证书状态',
            education_name VARCHAR(100) COMMENT '学历',
            crawl_date DATE NOT NULL COMMENT '爬取日期',
            crawl_time DATETIME COMMENT '爬取时间',
            create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            UNIQUE KEY uk_cert_code_date (cert_code, crawl_date)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '基金从业资格人员信息表'
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        # 保存数据（只保存最新1000条）
        df = pd.DataFrame(data)
        df_for_db = df.head(1000) if len(df) > 1000 else df
        current_date = date.today()
        
        with engine.connect() as conn:
            for _, row in df_for_db.iterrows():
                sql = """
                INSERT INTO fund_personnel
                (name, gender, cert_code, org_name, cert_name, cert_obtain_date,
                 cert_status_change_times, credit_record_num, status_name, education_name, crawl_date, crawl_time)
                VALUES (:name, :gender, :cert_code, :org_name, :cert_name, :cert_obtain_date,
                        :cert_status_change_times, :credit_record_num, :status_name, :education_name, :crawl_date, :crawl_time)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name), gender = VALUES(gender), org_name = VALUES(org_name),
                cert_name = VALUES(cert_name), cert_obtain_date = VALUES(cert_obtain_date),
                cert_status_change_times = VALUES(cert_status_change_times), credit_record_num = VALUES(credit_record_num),
                status_name = VALUES(status_name), education_name = VALUES(education_name), crawl_time = VALUES(crawl_time)
                """
                conn.execute(text(sql), {
                    'name': row['name'], 'gender': row['gender'], 'cert_code': row['cert_code'],
                    'org_name': row['org_name'], 'cert_name': row['cert_name'], 'cert_obtain_date': row['cert_obtain_date'],
                    'cert_status_change_times': row['cert_status_change_times'], 'credit_record_num': row['credit_record_num'],
                    'status_name': row['status_name'], 'education_name': row['education_name'],
                    'crawl_date': current_date, 'crawl_time': row['crawl_time']
                })
            conn.commit()
        
        logger.info(f"数据库保存成功: {len(df_for_db)}条数据")
        
    except Exception as e:
        logger.error(f"数据库保存失败: {e}")

def main():
    """主函数"""
    logger.info("=" * 50)
    
    # 爬取数据
    all_personnel = crawl_fund_data()
    
    if all_personnel:
        # 保存数据
        save_to_excel(all_personnel)
        save_to_database(all_personnel)
        
        # 输出结果
        current_date = date.today()
        logger.info(f"基金从业资格爬取完成: {len(all_personnel)} 条数据")
        logger.info(f"Excel文件: 基金从业资格_{current_date}.xlsx")
        logger.info(f"数据库表: fund_personnel")
    else:
        logger.error("基金从业资格爬取失败")
    
    logger.info("=" * 50)

if __name__ == "__main__":
    main() 