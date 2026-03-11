#!/usr/bin/env python3
"""
ArXiv客户端模块
负责论文搜索和下载功能，不再进行质量筛选，改为在AI分析阶段进行质量评估
"""

import datetime
from pathlib import Path
from typing import List, Optional

import arxiv
import requests # 确保导入 requests 以捕获其异常
import fitz  # PyMuPDF

from ..utils.logger import logger


class ArxivClient:
    """ArXiv客户端类"""

    def __init__(self, categories: List[str], max_papers: int = 50, search_days: int = 2, num_retries: int = 3, delay_seconds: float = 3.0):
        """
        初始化ArXiv客户端

        Args:
            categories: 论文类别列表
            max_papers: 最大论文数量
            search_days: 搜索最近几天的论文
            num_retries: arxiv.Client 请求的重试次数
            delay_seconds: arxiv.Client 请求之间的延迟秒数 (用于分页和重试)
        """
        self.categories = categories
        self.max_papers = max_papers
        self.search_days = search_days
        
        # 初始化 arxiv.py 库的客户端，并配置重试和延迟
        # arxiv.Client 会在内部处理分页请求之间的延迟 (delay_seconds)
        # 以及在请求失败时的重试 (num_retries)
        logger.info(f"Initializing arxiv.Client with num_retries={num_retries}, delay_seconds={delay_seconds}")
        self.arxiv_sdk_client = arxiv.Client(
            num_retries=num_retries,
            delay_seconds=delay_seconds
        )

    def get_recent_papers(self) -> List[arxiv.Result]:
        """
        获取最近几天内发布的指定类别的论文

        Returns:
            论文列表，按发布时间倒序排列
        """
        logger.info(f"ArxivClient: Initiating get_recent_papers. search_days = {self.search_days}")
        # 计算日期范围
        today_utc = datetime.datetime.now(datetime.UTC)  # 使用 timezone-aware UTC 时间
        start_date_utc = today_utc - datetime.timedelta(days=self.search_days)

        # 格式化ArXiv查询的日期
        start_date_str = start_date_utc.strftime("%Y%m%d")
        end_date_str = today_utc.strftime("%Y%m%d")
        logger.info(f"ArxivClient: Calculated date range for query: start_date_str = {start_date_str}, end_date_str = {end_date_str}")

        # 1. 定义关键词列表
        my_keywords = ["JEPA"]
        
        # 2. 构建关键词查询逻辑
        if len(my_keywords) > 1:
            # 多个关键词用 OR 连接
            keyword_query = "(" + " OR ".join([f'abs:"{kw}"' for kw in my_keywords]) + ")"
        else:
            # 单个关键词直接使用
            keyword_query = f'abs:"{my_keywords[0]}"'
        
        # 3. 类别逻辑 (同上)
        category_query = "(" + " OR ".join([f"cat:{cat}" for cat in self.categories]) + ")"
        
        # 4. 构建最终查询
        date_range = f"submittedDate:[{start_date_str}000000 TO {end_date_str}235959]"
        query = f"{category_query} AND {keyword_query} AND {date_range}"
        
        logger.info(f"正在搜索论文，查询条件: {query}")
        
        # 创建查询字符串
        # category_query = " OR ".join([f"cat:{cat}" for cat in self.categories])
        # date_range = f"submittedDate:[{start_date_str}000000 TO {end_date_str}235959]"
        # query = f"({category_query}) AND {date_range}"

        # logger.info(f"正在搜索论文，查询条件: {query}")

        # 搜索ArXiv
        search = arxiv.Search(
            query=query,
            max_results=self.max_papers,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        # 使用配置好的 arxiv.Client 实例获取结果
        try:
            results = list(self.arxiv_sdk_client.results(search))
            logger.info(f"找到{len(results)}篇符合条件的论文，将进行AI质量评估")
            return results
        except requests.exceptions.ConnectionError as e:
            logger.error(f"ArXiv连接错误 (Query: {query}): {e}")
            raise
        except Exception as e:
            logger.error(f"从ArXiv获取论文时发生未知错误 (Query: {query}): {e}")
            raise

    def download_paper(self, paper: arxiv.Result, output_dir: Path) -> Optional[Path]:
        """
        下载论文PDF到指定目录

        Args:
            paper: 论文对象
            output_dir: 输出目录

        Returns:
            PDF文件路径，下载失败返回None
        """
        pdf_path = output_dir / f"{paper.get_short_id().replace('/', '_')}.pdf"

        if pdf_path.exists():
            logger.info(f"论文已下载: {pdf_path}")
            return pdf_path

        try:
            logger.info(f"正在下载: {paper.title}")
            # 在 Result.download_pdf() 之前确保目录存在
            output_dir.mkdir(parents=True, exist_ok=True)
            # 下载操作也应该使用配置好的客户端，但 download_pdf 是 Result 对象的方法，
            # 它内部应该会复用 Client 的 session (如果设计合理) 或创建新的。
            # arxiv.py 的 Result.download_pdf() 似乎会创建一个临时的 Client(page_size=1, delay_seconds=0.0, num_retries=0)
            # 如果要让下载也享受重试，需要更深入的修改或手动下载。
            # 目前，我们将保持 Result.download_pdf() 的默认行为。
            paper.download_pdf(filename=str(pdf_path))
            logger.info(f"已下载到 {pdf_path}")
            return pdf_path
        except requests.exceptions.ConnectionError as e:
            logger.error(f"下载论文失败 (ConnectionError) {paper.title}: {e}")
            return None
        except arxiv.arxiv.ArxivError as e:
            logger.error(f"下载论文失败 (ArxivError) {paper.title}: {e}")
            return None
        except Exception as e:
            logger.error(f"下载论文失败 (Unknown Error) {paper.title}: {e.__class__.__name__} - {e}")
            return None

    def get_full_text(self, paper: arxiv.Result, output_dir: Path) -> Optional[str]:
        """
        下载PDF，提取全文，然后删除PDF。

        Args:
            paper: 论文对象
            output_dir: 临时下载目录

        Returns:
            论文全文的字符串，如果失败则返回None。
        """
        pdf_path = None
        try:
            logger.info(f"开始为论文 '{paper.title}' 提取全文...")
            pdf_path = self.download_paper(paper, output_dir)

            if not pdf_path or not pdf_path.exists():
                logger.error(f"下载失败或未找到PDF文件，无法提取文本: {pdf_path}")
                return None

            logger.info(f"从 {pdf_path} 提取文本...")
            full_text = ""
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    full_text += page.get_text()
            
            # 对提取的文本进行一些基本清理
            full_text = ' '.join(full_text.split())
            logger.info(f"成功为论文 '{paper.title}' 提取了 {len(full_text)} 字符的文本。")
            return full_text

        except Exception as e:
            logger.error(f"从PDF提取文本时出错: {e}", exc_info=True)
            return None
        finally:
            # 确保无论如何都尝试删除下载的PDF文件
            if pdf_path:
                self.delete_pdf(pdf_path)

    def delete_pdf(self, pdf_path: Path) -> None:
        """
        删除PDF文件

        Args:
            pdf_path: PDF文件路径
        """
        try:
            if pdf_path and pdf_path.exists():
                pdf_path.unlink()
                logger.info(f"已删除PDF文件: {pdf_path}")
            else:
                logger.info(f"PDF文件不存在，无需删除: {pdf_path}")
        except Exception as e:
            logger.error(f"删除PDF文件失败 {pdf_path}: {str(e)}")

    def filter_papers_by_keywords(
        self, papers: List[arxiv.Result], keywords: List[str] = None
    ) -> List[arxiv.Result]:
        """
        根据关键词过滤论文

        Args:
            papers: 论文列表
            keywords: 关键词列表

        Returns:
            过滤后的论文列表
        """
        if not keywords:
            return papers

        filtered_papers = []
        keywords_lower = [kw.lower() for kw in keywords]

        for paper in papers:
            title_lower = paper.title.lower()
            summary_lower = paper.summary.lower()

            if any(kw in title_lower or kw in summary_lower for kw in keywords_lower):
                filtered_papers.append(paper)

        logger.info(f"关键词过滤后剩余{len(filtered_papers)}篇论文")
        return filtered_papers
