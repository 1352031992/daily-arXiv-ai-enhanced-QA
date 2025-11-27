import os
import re
import scrapy


class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["arxiv.org"]

    # 为了让 QA 页先抓、再抓 RT 页（避免并发打乱全局顺序）
    custom_settings = {
        "CONCURRENT_REQUESTS": 1
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        categories = os.environ.get("CATEGORIES", "cs.CV")
        # 目标分类（去空格）
        cats = [c.strip() for c in categories.split(",") if c.strip()]

        # 分类优先级：QA 在 RT 前
        self.CAT_PRIORITY = {"math.QA": 0, "math.RT": 1}
        # start_urls 按优先级排序（未知分类放最后）
        cats.sort(key=lambda c: self.CAT_PRIORITY.get(c, 99))

        self.target_categories = set(cats)
        self.start_urls = [f"https://arxiv.org/list/{cat}/new" for cat in cats]

        # 全局去重，避免 QA/RT 交叉时同一篇重复
        self.seen_ids = set()

    def parse(self, response):
        """
        需求：
        1) math.QA 在 math.RT 前（由 __init__ + CONCURRENT_REQUESTS=1 保证页面处理顺序）
        2) 每个分类内：New submissions -> Cross-lists -> Replacements
        3) 同层内：按 arXiv 编号倒序
        """
        # 从当前 URL 提取“来源分类”，用于学科优先级
        # 形如 https://arxiv.org/list/math.QA/new
        mcat = re.search(r"/list/([^/]+)/new", response.url)
        source_cat = mcat.group(1) if mcat else ""
        cat_priority = self.CAT_PRIORITY.get(source_cat, 99)

        page_items = []

        # 记录当前分区文本（new/cross/repl/other）
        current_section_text = "other"
        current_section_rank = 3

        # 遍历 #dlpage 下 h3/dl 的交替结构，识别区块标题
        # 使用 xpath 保证顺序：h3 -> dl -> h3 -> dl ...
        for section in response.xpath("//div[@id='dlpage']/*[self::h3 or self::dl]"):
            tag = section.root.tag.lower()

            # 识别区块类型，映射成排序键
            if tag == "h3":
                heading = "".join(section.css("::text").getall()).strip()
                heading_lower = heading.lower()

                # 兼容复数/变体：New submissions / Cross-lists / Replacements
                if re.search(r"\bnew submissions?\b", heading_lower):
                    current_section_rank = 0
                    current_section_text = "new"
                elif re.search(r"\bcross-?lists?\b", heading_lower) or re.search(r"\bcross submissions?\b", heading_lower):
                    current_section_rank = 1
                    current_section_text = "cross"
                elif re.search(r"\breplacements?\b", heading_lower) or re.search(r"\breplacement submissions?\b", heading_lower):
                    current_section_rank = 2
                    current_section_text = "repl"
                else:
                    current_section_rank = 3
                    current_section_text = "other"
                continue

            if tag != "dl":
                continue

            # 逐条解析该区块里的 dt/dd
            dts = section.css("dt")
            dds = section.css("dd")
            for paper_dt, paper_dd in zip(dts, dds):
                # ---- arXiv id ----
                abs_href = paper_dt.css("a[title='Abstract']::attr(href)").get()
                if not abs_href:
                    abs_href = paper_dt.css("a[href*='/abs/']::attr(href)").get()
                if not abs_href:
                    continue

                abs_url = response.urljoin(abs_href)
                mid = re.search(r"/abs/([0-9]{4}\.[0-9]{5})", abs_url)
                if not mid:
                    continue
                arxiv_id = mid.group(1)

                # 去重（跨分类/跨区块）—— 仍按“裸 id”控制调度，避免重复请求
                if arxiv_id in self.seen_ids:
                    continue
                self.seen_ids.add(arxiv_id)

                # ---- 学科解析（包含 cross-list）----
                subj_parts = paper_dd.css(".list-subjects ::text").getall()
                subjects_text = " ".join(t.strip() for t in subj_parts if t.strip())

                # 只提取学科代码，如 (math.QA)、(math.RT)、(math-ph)、(cs.CV)
                code_regex = r"\(([a-z\-]+\.[A-Z]{2})\)"
                categories_in_paper = re.findall(code_regex, subjects_text)
                paper_categories = set(categories_in_paper)

                # 命中任一目标分类才收；否则仅在“完全取不到学科”时兜底收录
                if paper_categories.intersection(self.target_categories) or not subjects_text:
                    # 进入 abs 页面，**从 canonical 链接取 version**
                    meta_item = {
                        "id": arxiv_id,
                        "section": current_section_text,         # new/cross/repl/other
                        "abs": abs_url,
                        "pdf": abs_url.replace("/abs/", "/pdf/"),
                        "categories": list(paper_categories) if subjects_text else [],
                        # 排序键
                        "cat_priority": cat_priority,
                        "section_rank": current_section_rank,
                    }
                    yield response.follow(
                        abs_url,
                        callback=self.parse_abs,
                        cb_kwargs={"meta_item": meta_item},
                        dont_filter=True
                    )
                else:
                    self.logger.debug(
                        f"Skipped {arxiv_id} with categories {paper_categories} "
                        f"(target: {self.target_categories})"
                    )

        # 其余排序在 parse_abs 内最后统一完成

    def parse_abs(self, response, meta_item):
        """
        在 abs 页面中确定 version（最稳：<link rel='canonical' href='.../abs/xxxxvN'>）
        并把条目送出。所有条目先缓存在 self._buffer，页末统一排序再 yield。
        """
        # 1) 版本号：优先 canonical 链接，其次 h1 标题，最后兜底 v1
        version = "v1"
        canonical = response.xpath("//link[@rel='canonical']/@href").get()
        if canonical:
            m = re.search(r"/abs/\d{4}\.\d{5}(v\d+)$", canonical)
            if m:
                version = m.group(1)
        if version == "v1":
            # 次选：h1 标题（有时包含 vN）
            h1 = " ".join(response.xpath("//h1//text()").getall())
            m2 = re.search(r"\b(v\d+)\b", h1)
            if m2:
                version = m2.group(1)

        item = dict(meta_item)
        item["version"] = version

        # 缓存条目以便排序
        buf = getattr(self, "_buffer", [])
        buf.append(item)
        self._buffer = buf

        # 尝试判断是否该页面的所有请求都结束：当 _pending_requests 归零时输出
        # 简化处理：当 buffer 累积达到 seen_ids 的数量时统一输出
        if len(self._buffer) >= len(self.seen_ids):
            # ===== 排序 =====
            # 规则：分类优先级(升) -> 区块(New=0, Cross=1, Replacements=2, 其余=3)(升) -> arXiv编号(降)
            items = self._buffer
            items.sort(key=lambda x: x["id"], reverse=True)
            items.sort(key=lambda x: x["section_rank"])
            items.sort(key=lambda x: x["cat_priority"])
            for it in items:
                it.pop("cat_priority", None)
                it.pop("section_rank", None)
                yield it

            # 清空 buffer，避免别的分类混入
            self._buffer = []
