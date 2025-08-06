#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金从业资格爬虫定时任务
每天早上7点运行，8点前完成
"""

import schedule
import time
import subprocess
import sys
import os
from datetime import datetime
import logging

def setup_logging():
    """设置日志"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.FileHandler(f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def run_crawler():
    """运行爬虫任务"""
    try:
        logger.info("开始执行基金从业资格爬虫定时任务")
        
        # 运行爬虫脚本
        result = subprocess.run([sys.executable, 'fund_crawler_post.py'], 
                              capture_output=True, text=True, encoding='utf-8')
        
        if result.returncode == 0:
            logger.info("✅ 爬虫任务执行成功")
        else:
            logger.error("❌ 爬虫任务执行失败")
            logger.error(f"错误: {result.stderr}")
        
    except Exception as e:
        logger.error(f"定时任务执行异常: {e}")

def main():
    """主函数"""
    logger.info("基金从业资格爬虫定时任务启动")
    logger.info("定时设置: 每天早上7:00执行")
    
    # 设置定时任务 - 每天早上7点执行
    schedule.every().day.at("07:00").do(run_crawler)
    
    # 如果当前时间在7点到8点之间，立即执行一次
    current_hour = datetime.now().hour
    if 7 <= current_hour < 8:
        logger.info("当前时间在7-8点之间，立即执行一次爬虫任务")
        run_crawler()
    
    logger.info("定时任务已启动，等待执行...")
    
    # 持续运行，检查定时任务
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("定时任务被手动停止")
            break
        except Exception as e:
            logger.error(f"定时任务异常: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main() 