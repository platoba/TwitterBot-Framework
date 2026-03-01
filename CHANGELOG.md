# Changelog

## v6.0.0 (2026-03-01)

### 🚀 New Modules

- **Audience Lookalike** (`bot/audience_lookalike.py`): 受众Lookalike发现引擎
  - UserProfile: 用户画像数据模型 (粉丝/互动率/兴趣标签/来源追踪)
  - InterestExtractor: 7大兴趣分类自动提取 (tech/marketing/ecommerce/finance/design/creator/business) + 相似度计算
  - SimilarityCalculator: 4种相似度算法 (Jaccard/Cosine/Overlap/Dice)
  - LookalikeStore: SQLite持久化 (种子账号/发现用户/重叠缓存 + 索引)
  - AudienceLookalike: 完整发现引擎 (种子粉丝收集→多维评分→5分段分类→行动推荐→Bot过滤)
  - 5种受众分段: HIGH_VALUE/WARM_LEAD/COLD_LEAD/COMPETITOR_FAN/INDUSTRY_PEER
  - 粉丝重叠分析: 两两种子账号Jaccard/重叠比/独占用户
  - JSON/CSV导出 + 分段摘要统计

- **Content Moderation** (`bot/content_moderation.py`): 内容安全审核管道
  - ToxicityDetector: 4级毒性检测 (CRITICAL/HIGH/MEDIUM/LOW) + 正则模式库
  - PIIDetector: 8类PII识别 (email/phone_us/phone_cn/SSN/credit_card/IP/passport/ID_card_cn) + 自动脱敏redact
  - BrandSafetyChecker: 3级品牌安全关键词 + 自定义词表扩展
  - SpamDetector: 7种垃圾指标 (hashtag泛滥/大写轰炸/金钱诱饵/URL洪水/关注乞求/DM诱骗/重复字符)
  - PlatformComplianceChecker: Twitter/X平台合规 (280字符/hashtag/mention上限)
  - LinkSafetyChecker: 可疑TLD检测 + 钓鱼URL模式匹配
  - ModerationStore: SQLite审核日志 + 每日统计 (approval_rate)
  - ContentModerator: 统一审核管道 (6检测器→风险评分→自动拒绝/修复→日志)

- **Link Intelligence** (`bot/link_intelligence.py`): 链接智能管理引擎
  - UTMParams: UTM参数模型 + URL提取 + Twitter默认
  - UTMBuilder: URL构建 (添加UTM) + 清除UTM + 参数验证
  - ShortCodeGenerator: 62字符随机短码 + 种子确定性 + URL哈希 + 格式校验
  - LinkStore: SQLite持久化 (链接/点击事件/分组 + 索引) + 聚合分析
  - TrackedLink: 追踪链接模型 (短码/UTM/点击/状态/标签/过期)
  - LinkIntelligence: 完整引擎 (创建追踪链接→记录点击→设备/平台检测→过期检查→Campaign报告)
  - 批量创建 + 文本链接替换 + JSON/CSV导出

### 📊 Testing

- 新增 168 个测试 (3个测试文件)
  - `tests/test_audience_lookalike.py`: 61 tests
  - `tests/test_content_moderation.py`: 53 tests
  - `tests/test_link_intelligence.py`: 54 tests
- 总测试数: **2076** (1908 → 2076)
- 全部通过 ✅

## v5.0.0 (2026-03-01)

### 🚀 New Modules

- **Monetization Tracker** (`bot/monetization.py`): 推文变现追踪引擎
  - LinkDetector: 8大联盟平台自动检测 (Amazon/Shopify/AliExpress/eBay/ClickBank/CJ/ShareASale/Impact)
  - MonetizationStore: 链接/收入/点击三维数据存储 + 推文关联
  - ROICalculator: 单推文/活动ROI + 平台收益分解 + 每日汇总
  - MonetizationReport: 完整报告 (Top链接/Top推文) + CSV/JSON/Text导出
  - MonetizationEngine: 统一入口 (处理推文→检测链接→追踪点击→记录收入→计算ROI)

- **DM Funnel** (`bot/dm_funnel.py`): 自动化私信漏斗引擎
  - 5种步骤类型: Message/Delay/Condition/Action/Tag
  - 8种触发条件: new_follower/keyword/reply/retweet/like/mention/manual/webhook
  - 8种条件运算: equals/contains/gt/lt/in/not_in/regex/exists
  - TemplateEngine: {{var}} 模板渲染
  - ConditionEvaluator: 分支逻辑评估器
  - 用户状态追踪: 标签系统 + 变量存储 + 消息计数 + 历史记录
  - 漏斗统计: 完成率/退出率/回复率 + JSON导入导出

- **Profile Optimizer** (`bot/profile_optimizer.py`): AI驱动Profile优化器
  - BioAnalyzer: 8维文本分析 (词数/emoji/hashtag/power words/CTA/social proof/格式/可读性)
  - ProfileScorer: 8项评分 (Bio长度/Power词/CTA/社交证明/Emoji/格式/完整度/互动比) = 100分
  - S/A+/A/B+/B/C/D/F 八级评定
  - ProfileComparator: 竞品Profile对比 + 排名
  - BioGenerator: 5种模板 + Bio建议生成 + 长度优化
  - ProfileOptimizer: 统一入口 + 文本报告

### 📊 Test Coverage

- New tests: +203 (3 test files)
  - `tests/test_monetization.py`: 79 tests (link detection, store, ROI, reports, E2E)
  - `tests/test_dm_funnel.py`: 66 tests (CRUD, triggers, execution, conditions, analytics)
  - `tests/test_profile_optimizer.py`: 58 tests (analysis, scoring, comparison, generation)
- Total: 1434 → 1637 tests

### 📝 New Code

- +1600 lines across 3 modules + 3 test files

## v4.0.0 (2026-03-01)

### 🚀 New Modules

- **Influencer Finder** (`bot/influencer_finder.py`): KOL发现引擎
  - NicheScorer: 基于关键词/话题的垂类相关度评分
  - EngagementQualityAnalyzer: 区分真互动 vs 水军 (5项异常检测)
  - GrowthTracker: 粉丝增长轨迹分析 + SQLite持久化
  - CooperationEstimator: 合作ROI预估 (CPE/报价/效率评分)
  - InfluencerRanker: 5维加权综合排名 (niche/quality/growth/cooperation/authenticity)
  - WatchList: 关注列表持久化 + 变动事件追踪
  - 完整JSON/CSV/Text报告导出

- **Content Recycler** (`bot/content_recycler.py`): 内容回收再利用引擎
  - PerformanceScanner: 历史高表现推文扫描 + 常青内容识别
  - FreshnessChecker: 9类内容时效性检查 + 自动分类
  - StrategySuggester: 8种改写策略推荐 (quote/update/thread/QA/listicle/visual/reverse/summary)
  - RecycleScheduler: 回收调度 + 冷却期管理 + 原版vs回收版表现对比
  - 智能改写提示词生成

- **Trend Tracker** (`bot/trend_tracker.py`): 趋势追踪引擎
  - BurstDetector: Z-score爆发检测 + 移动窗口异常检测
  - RelevanceEngine: 趋势与Niche相关度评估 (关键词/hashtag/推文内容多维匹配)
  - OpportunityScorer: 参与时机评分 + 4级优先级分级 (critical/high/medium/low)
  - TrendHistory: SQLite趋势归档 + 周期性趋势发现 + 热词统计
  - TrendAlert: 4类预警 (新趋势/阶段变化/量级突增/高机会窗口)
  - 内容建议生成 (first_mover/hot_take/thread/hashtag_ride)

### 📊 Test Coverage
- 新增146个测试 (3个测试文件): 849 → 995个测试
- test_influencer_finder.py: 61个测试
- test_content_recycler.py: 46个测试
- test_trend_tracker.py: 39个测试

### 📈 Code Growth
- 新增3个核心模块 + 3个测试文件
- 新增代码: ~3,600行 (模块) + ~1,800行 (测试)
- Python文件: 36 → 39个

## v3.0.0 (2026-02-28)

### 🚀 New Modules

#### Multi-Account Manager (`bot/multi_account.py`)
- Account pool management with credential rotation
- Per-account independent rate limit tracking (tweets/DMs/search/follows)
- Account health scoring (engagement rate + growth + violations → 0-100)
- Automatic failover when account is rate-limited or erroring
- Round-robin rotation among available accounts
- Account grouping by role (main/backup/niche/engagement/monitoring)
- Event logging with SQLite persistence
- Aggregated cross-account analytics
- Bulk status management + daily counter reset
- Masked credential export for safe backup

#### Media Manager (`bot/media_manager.py`)
- Twitter media validation (format/size/dimensions per type)
- Alt text generator with 9 templates (product/person/chart/screenshot/meme/infographic/logo/landscape/default)
- Media library with SQLite persistence, tagging & search
- Upload queue with configurable retry logic
- Duplicate detection by checksum
- Media usage tracking & analytics
- Watermark engine configuration
- Optimization suggestions (resize/compress/aspect ratio/alt text)
- GIF/video constraint validation

#### Engagement Rules Engine (`bot/engagement_rules.py`)
- Declarative rule definitions with conditions → actions
- 18 condition types (keyword/hashtag/follower/engagement/language/media/link/verified/time window...)
- 9 action types (like/retweet/reply/follow/bookmark/mute/block/notify/tag)
- Rule priority system (LOW→CRITICAL) with conflict resolution
- Safety guardrails: daily limits, min intervals, blocklist, protected authors
- Probabilistic action execution
- Cooldown & daily trigger limits per rule
- Full action history with SQLite logging
- 6 pre-built rule templates:
  - Niche engagement (keyword + follower threshold)
  - Influencer engage (high-follower + bookmark)
  - Smart follow-back (follower range filter)
  - Viral amplify (high-engagement retweet)
  - Spam filter (keyword → mute, CRITICAL priority)
  - Hashtag engage (hashtag-based auto-like)

### 📊 Tests
- **182 new tests** (723 total, all passing)
  - `test_multi_account.py`: 64 tests
  - `test_media_manager.py`: 57 tests
  - `test_engagement_rules.py`: 61 tests

## v2.0.0 (2026-02-27)
- Strategy engine + analytics + content generator + webhook + rate limiter + SQLite

## v1.0.0 (2026-02-27)
- Initial Twitter/X automation via TG Bot
