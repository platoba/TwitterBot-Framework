"""
Twitter API v2 封装层
支持限流保护 + 自动重试 + Bearer/OAuth双模式
"""

import os
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import requests

logger = logging.getLogger(__name__)


class RateLimiter:
    """Twitter API限流保护器"""

    def __init__(self):
        self._limits: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def update(self, endpoint: str, headers: Dict[str, str]):
        """从响应头更新限流状态"""
        with self._lock:
            remaining = headers.get("x-rate-limit-remaining")
            reset_ts = headers.get("x-rate-limit-reset")
            limit = headers.get("x-rate-limit-limit")

            if remaining is not None and reset_ts is not None:
                self._limits[endpoint] = {
                    "remaining": int(remaining),
                    "reset": int(reset_ts),
                    "limit": int(limit) if limit else 0,
                    "updated_at": time.time()
                }

    def check(self, endpoint: str) -> Tuple[bool, float]:
        """检查是否可以调用, 返回 (can_proceed, wait_seconds)"""
        with self._lock:
            info = self._limits.get(endpoint)
            if info is None:
                return True, 0

            if info["remaining"] > 0:
                return True, 0

            now = time.time()
            reset_at = info["reset"]
            if now >= reset_at:
                return True, 0

            wait = reset_at - now + 1
            return False, wait

    def wait_if_needed(self, endpoint: str) -> float:
        """如果需要等待, 自动等待并返回等待秒数"""
        can_proceed, wait_seconds = self.check(endpoint)
        if not can_proceed and wait_seconds > 0:
            capped = min(wait_seconds, 900)
            logger.warning(f"Rate limited on {endpoint}, waiting {capped:.0f}s")
            time.sleep(capped)
            return capped
        return 0

    def get_status(self) -> Dict[str, Dict]:
        """返回所有端点限流状态"""
        with self._lock:
            return dict(self._limits)


class TwitterAPI:
    """Twitter API v2 客户端"""

    BASE_URL = "https://api.twitter.com/2"

    def __init__(self, bearer_token: str = None,
                 api_key: str = None, api_secret: str = None,
                 access_token: str = None, access_secret: str = None,
                 max_retries: int = 3):
        self.bearer_token = bearer_token or os.environ.get("TW_BEARER_TOKEN", "")
        self.api_key = api_key or os.environ.get("TW_API_KEY", "")
        self.api_secret = api_secret or os.environ.get("TW_API_SECRET", "")
        self.access_token = access_token or os.environ.get("TW_ACCESS_TOKEN", "")
        self.access_secret = access_secret or os.environ.get("TW_ACCESS_SECRET", "")
        self.max_retries = max_retries
        self.rate_limiter = RateLimiter()
        self._session = requests.Session()

    @property
    def is_configured(self) -> bool:
        return bool(self.bearer_token)

    @property
    def can_write(self) -> bool:
        """是否能执行写操作(需要OAuth 1.0a)"""
        return bool(self.api_key and self.api_secret and
                     self.access_token and self.access_secret)

    def _bearer_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.bearer_token}"}

    def _oauth_headers(self, method: str, url: str, params: Dict = None) -> Dict[str, str]:
        """生成OAuth 1.0a签名 (简化版, 生产环境建议用 requests-oauthlib)"""
        try:
            from requests_oauthlib import OAuth1
            auth = OAuth1(self.api_key, self.api_secret,
                          self.access_token, self.access_secret)
            return {"auth_obj": auth}
        except ImportError:
            return self._bearer_headers()

    def _request(self, method: str, endpoint: str, params: Dict = None,
                 json_data: Dict = None, use_oauth: bool = False) -> Optional[Dict]:
        """通用请求方法, 带限流保护和重试"""
        url = f"{self.BASE_URL}{endpoint}"

        self.rate_limiter.wait_if_needed(endpoint)

        headers = {}
        auth = None
        if use_oauth and self.can_write:
            oauth_result = self._oauth_headers(method, url, params)
            if "auth_obj" in oauth_result:
                auth = oauth_result["auth_obj"]
            else:
                headers = oauth_result
        else:
            headers = self._bearer_headers()

        for attempt in range(self.max_retries):
            try:
                resp = self._session.request(
                    method, url,
                    params=params,
                    json=json_data,
                    headers=headers,
                    auth=auth,
                    timeout=15
                )

                self.rate_limiter.update(endpoint, dict(resp.headers))

                if resp.status_code == 429:
                    wait = self.rate_limiter.wait_if_needed(endpoint)
                    if wait == 0:
                        time.sleep(min(60 * (attempt + 1), 300))
                    continue

                if resp.status_code >= 500:
                    time.sleep(5 * (attempt + 1))
                    continue

                if resp.ok:
                    return resp.json()
                else:
                    logger.error(f"Twitter API error {resp.status_code}: {resp.text[:200]}")
                    return {"error": resp.status_code, "detail": resp.text[:500]}

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on {endpoint} (attempt {attempt + 1})")
                time.sleep(5)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                time.sleep(5)

        return None

    def get(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, json_data: Dict = None) -> Optional[Dict]:
        return self._request("POST", endpoint, json_data=json_data, use_oauth=True)

    def delete(self, endpoint: str) -> Optional[Dict]:
        return self._request("DELETE", endpoint, use_oauth=True)

    # ── 搜索 ──

    def search_recent(self, query: str, max_results: int = 10,
                       since_id: str = None) -> Optional[Dict]:
        params = {
            "query": query,
            "max_results": min(max(max_results, 10), 100),
            "tweet.fields": "public_metrics,created_at,author_id,conversation_id",
            "expansions": "author_id",
            "user.fields": "username,public_metrics,profile_image_url"
        }
        if since_id:
            params["since_id"] = since_id
        return self.get("/tweets/search/recent", params)

    # ── 用户 ──

    def get_user(self, username: str) -> Optional[Dict]:
        return self.get(f"/users/by/username/{username}", {
            "user.fields": "public_metrics,description,created_at,profile_image_url,verified"
        })

    def get_user_by_id(self, user_id: str) -> Optional[Dict]:
        return self.get(f"/users/{user_id}", {
            "user.fields": "public_metrics,description,created_at,profile_image_url,verified"
        })

    def get_user_tweets(self, user_id: str, max_results: int = 10,
                         since_id: str = None) -> Optional[Dict]:
        params = {
            "max_results": min(max(max_results, 5), 100),
            "tweet.fields": "public_metrics,created_at",
            "exclude": "retweets"
        }
        if since_id:
            params["since_id"] = since_id
        return self.get(f"/users/{user_id}/tweets", params)

    def get_user_mentions(self, user_id: str, max_results: int = 10,
                           since_id: str = None) -> Optional[Dict]:
        params = {
            "max_results": min(max(max_results, 5), 100),
            "tweet.fields": "public_metrics,created_at,author_id",
            "expansions": "author_id",
            "user.fields": "username"
        }
        if since_id:
            params["since_id"] = since_id
        return self.get(f"/users/{user_id}/mentions", params)

    def get_user_followers(self, user_id: str, max_results: int = 100) -> Optional[Dict]:
        return self.get(f"/users/{user_id}/followers", {
            "max_results": min(max_results, 1000),
            "user.fields": "public_metrics,description,created_at"
        })

    # ── 推文操作 (需要OAuth) ──

    def post_tweet(self, text: str, reply_to: str = None,
                    quote_tweet_id: str = None) -> Optional[Dict]:
        payload: Dict[str, Any] = {"text": text}
        if reply_to:
            payload["reply"] = {"in_reply_to_tweet_id": reply_to}
        if quote_tweet_id:
            payload["quote_tweet_id"] = quote_tweet_id
        return self.post("/tweets", payload)

    def delete_tweet(self, tweet_id: str) -> Optional[Dict]:
        return self.delete(f"/tweets/{tweet_id}")

    def like_tweet(self, user_id: str, tweet_id: str) -> Optional[Dict]:
        return self.post(f"/users/{user_id}/likes", {"tweet_id": tweet_id})

    def retweet(self, user_id: str, tweet_id: str) -> Optional[Dict]:
        return self.post(f"/users/{user_id}/retweets", {"tweet_id": tweet_id})

    # ── 趋势 ──

    def get_trends(self, woeid: int = 1) -> Optional[Dict]:
        return self.get("/trends/by/woeid", {"woeid": woeid})

    # ── 便捷方法 ──

    def resolve_username(self, username: str) -> Optional[str]:
        data = self.get_user(username)
        if data and "data" in data:
            return data["data"]["id"]
        return None

    def get_rate_limit_status(self) -> Dict:
        return self.rate_limiter.get_status()
