import requests
import json
import pandas as pd
import time
import re
from sqlalchemy import create_engine, text, exc
from datetime import datetime, date
from urllib.parse import urlencode

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xjtuse',
    'database': 'qs',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

def get_database_engine():
    """创建数据库连接"""
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
            f"{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
        )
        with engine.connect():
            print("数据库连接成功")
        return engine
    except exc.SQLAlchemyError as e:
        print(f"数据库连接失败: {str(e)}")
        return None

def create_table_if_not_exists(engine):
    """创建数据表（如果不存在）"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS university_rank (
        id INT AUTO_INCREMENT PRIMARY KEY,
        university_name VARCHAR(255) NOT NULL,
        `rank` INT NOT NULL,
        overall_score DECIMAL(10, 2),
        location VARCHAR(255),
        country VARCHAR(255),
        city VARCHAR(255),
        region VARCHAR(255),
        logo_url TEXT,
        path VARCHAR(500),
        crawl_date DATE NOT NULL,
        create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_crawl_date (crawl_date),
        INDEX idx_university (university_name),
        INDEX idx_rank (`rank`),
        INDEX idx_country (country)
    ) ENGINE = InnoDB DEFAULT CHARSET = :charset COLLATE = :collation
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql), {
                "charset": DB_CONFIG['charset'],
                "collation": DB_CONFIG['collation']
            })
            conn.commit()
            print("数据表检查/创建完成")
    except exc.SQLAlchemyError as e:
        print(f"表操作失败: {str(e)}")
        raise

def get_session():
    """创建requests会话，设置请求头"""
    session = requests.Session()
    
    # 设置请求头，模拟真实浏览器
    headers = {
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
    }
    
    session.headers.update(headers)
    return session

def extract_data_from_api(session, items_per_page=30):
    """从API接口提取数据"""
    universities = []
    
    try:
        # QS排名API接口
        api_url = "https://www.topuniversities.com/rankings/endpoint"
        
        # 请求参数
        params = {
            'nid': '3990755',
            'page': 0,
            'items_per_page': items_per_page,
            'tab': 'indicators',
            'region': '',
            'countries': '',
            'cities': '',
            'search': '',
            'star': '',
            'sort_by': '',
            'order_by': '',
            'program_type': '',
            'scholarship': '',
            'fee': '',
            'english_score': '',
            'academic_score': '',
            'mix_student': '',
            'loggedincache': '6905039-1754356589358'
        }
        
        print(f"正在请求API: {api_url}")
        print(f"请求参数: {params}")
        
        response = session.get(api_url, params=params, timeout=30)
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"API响应成功，数据类型: {type(data)}")
                print(f"响应键: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                
                # 解析JSON数据
                if 'score_nodes' in data:
                    universities_data = data['score_nodes']
                    print(f"找到 {len(universities_data)} 个大学数据")
                    
                    for uni in universities_data:
                        try:
                            # 提取基本信息
                            university_info = {
                                'rank': int(uni.get('rank', 0)),
                                'overall_score': float(uni.get('overall_score', 0)) if uni.get('overall_score') else None,
                                'university_name': uni.get('title', ''),
                                'location': f"{uni.get('city', '')}, {uni.get('country', '')}".strip(', '),
                                'country': uni.get('country', ''),
                                'city': uni.get('city', ''),
                                'region': uni.get('region', ''),
                                'logo_url': uni.get('logo', ''),
                                'path': uni.get('path', '')
                            }
                            
                            universities.append(university_info)
                            
                        except Exception as e:
                            print(f"解析单条数据失败: {e}")
                            continue
                    
                    print(f"从API成功提取 {len(universities)} 条数据")
                    
                else:
                    print("未找到 'score_nodes' 键")
                    print(f"可用的键: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    return pd.DataFrame()
                
            except json.JSONDecodeError as e:
                print(f"JSON解析失败: {e}")
                print(f"响应内容: {response.text[:500]}...")
                return pd.DataFrame()
                
        else:
            print(f"API请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text[:200]}...")
            
    except requests.exceptions.RequestException as e:
        print(f"请求异常: {e}")
    except Exception as e:
        print(f"数据提取失败: {e}")
    
    return pd.DataFrame(universities)

def extract_data_from_js_url(session, js_url):
    """从JS动态加载的URL提取数据"""
    universities = []
    
    try:
        print(f"正在请求JS数据: {js_url}")
        response = session.get(js_url, timeout=30)
        
        if response.status_code == 200:
            # 尝试解析JSON
            try:
                data = response.json()
                print(f"JS数据解析成功，数据类型: {type(data)}")
                
                # 根据实际数据结构解析
                if isinstance(data, dict) and 'score_nodes' in data:
                    universities_data = data['score_nodes']
                    print(f"找到 {len(universities_data)} 个大学数据")
                    
                    for uni in universities_data:
                        try:
                            # 提取基本信息
                            university_info = {
                                'rank': int(uni.get('rank', 0)),
                                'overall_score': float(uni.get('overall_score', 0)) if uni.get('overall_score') else None,
                                'university_name': uni.get('title', ''),
                                'location': f"{uni.get('city', '')}, {uni.get('country', '')}".strip(', '),
                                'country': uni.get('country', ''),
                                'city': uni.get('city', ''),
                                'region': uni.get('region', ''),
                                'logo_url': uni.get('logo', ''),
                                'path': uni.get('path', '')
                            }
                            
                            universities.append(university_info)
                            
                        except Exception as e:
                            print(f"解析单条数据失败: {e}")
                            continue
                    
                    print(f"从JS URL成功提取 {len(universities)} 条数据")
                    
                else:
                    print("数据结构不符合预期")
                    print(f"可用的键: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    return pd.DataFrame()
                
            except json.JSONDecodeError as e:
                print(f"JSON解析失败: {e}")
                print(f"响应内容: {response.text[:200]}...")
                return pd.DataFrame()
                
        else:
            print(f"JS请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text[:200]}...")
            
    except requests.exceptions.RequestException as e:
        print(f"请求异常: {e}")
    except Exception as e:
        print(f"数据提取失败: {e}")
    
    return pd.DataFrame(universities)

def save_to_database(df, engine, crawl_date):
    """保存数据到数据库"""
    if df.empty or engine is None:
        print("无数据可保存")
        return False
    
    df['crawl_date'] = crawl_date
    
    try:
        with engine.connect() as conn:
            # 检查是否已有该日期的数据
            result = conn.execute(
                text("SELECT COUNT(*) FROM university_rank WHERE crawl_date = :date"),
                {"date": crawl_date}
            )
            if result.scalar() > 0:
                print(f"该日期({crawl_date})已存在数据")
                return False
            
            # 直接插入数据
            df.to_sql('university_rank', conn, if_exists='append', index=False)
            conn.commit()
            print(f"成功保存 {len(df)} 条数据到数据库")
            return True
    except exc.SQLAlchemyError as e:
        print(f"数据库保存失败: {str(e)}")
        return False

def scrape_qs_rankings_requests(js_url=None, items_per_page=30):
    """使用requests爬取QS大学排名数据"""
    # 初始化数据库
    engine = get_database_engine()
    if not engine:
        return
    
    create_table_if_not_exists(engine)
    
    # 设置爬取日期
    crawl_date = date.today().replace(day=1)
    print(f"爬取日期: {crawl_date}")
    
    # 创建会话
    session = get_session()
    
    try:
        df = pd.DataFrame()
        
        if js_url:
            # 使用提供的JS URL
            print("使用提供的JS URL提取数据...")
            df = extract_data_from_js_url(session, js_url)
        else:
            # 尝试API接口
            print("尝试API接口提取数据...")
            df = extract_data_from_api(session, items_per_page)
        
        if not df.empty:
            # 显示数据预览
            print("\n📊 数据预览:")
            print(df.head())
            
            # 保存到Excel
            excel_filename = f'QS大学排名_requests_{crawl_date}.xlsx'
            df.to_excel(excel_filename, index=False)
            print(f"数据已保存到 {excel_filename}")
            
            # 保存到数据库
            save_to_database(df, engine, crawl_date)
        else:
            print("未提取到任何数据")
            
    except Exception as e:
        print(f"爬取过程出错: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    # 如果您有JS URL，请在这里提供
    js_url = "https://www.topuniversities.com/rankings/endpoint?nid=3990755&page=0&items_per_page=30&tab=indicators&region=&countries=&cities=&search=&star=&sort_by=&order_by=&program_type=&scholarship=&fee=&english_score=&academic_score=&mix_student=&loggedincache=6905039-1754356589358"  # 例如: "https://api.example.com/rankings"
    
    scrape_qs_rankings_requests(js_url) 