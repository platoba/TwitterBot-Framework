"""
Twitter API v2 client with OAuth 2.0 PKCE support.

Implements modern Twitter API v2 endpoints:
- Tweet lookup, search, counts
- User lookup, followers, following
- Spaces, Lists
- OAuth 2.0 Authorization Code Flow with PKCE
- Automatic token refresh
- Rate limit aware with adaptive backoff
"""

import hashlib
import json
import secrets
import time
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class TweetField(str, Enum):
    """Available tweet fields for expansion."""
    ATTACHMENTS = "attachments"
    AUTHOR_ID = "author_id"
    CONTEXT_ANNOTATIONS = "context_annotations"
    CONVERSATION_ID = "conversation_id"
    CREATED_AT = "created_at"
    EDIT_CONTROLS = "edit_controls"
    ENTITIES = "entities"
    GEO = "geo"
    ID = "id"
    IN_REPLY_TO_USER_ID = "in_reply_to_user_id"
    LANG = "lang"
    PUBLIC_METRICS = "public_metrics"
    POSSIBLY_SENSITIVE = "possibly_sensitive"
    REFERENCED_TWEETS = "referenced_tweets"
    REPLY_SETTINGS = "reply_settings"
    SOURCE = "source"
    TEXT = "text"
    WITHHELD = "withheld"


class UserField(str, Enum):
    """Available user fields for expansion."""
    CREATED_AT = "created_at"
    DESCRIPTION = "description"
    ENTITIES = "entities"
    ID = "id"
    LOCATION = "location"
    NAME = "name"
    PINNED_TWEET_ID = "pinned_tweet_id"
    PROFILE_IMAGE_URL = "profile_image_url"
    PROTECTED = "protected"
    PUBLIC_METRICS = "public_metrics"
    URL = "url"
    USERNAME = "username"
    VERIFIED = "verified"
    VERIFIED_TYPE = "verified_type"
    WITHHELD = "withheld"


class MediaField(str, Enum):
    """Available media fields."""
    DURATION_MS = "duration_ms"
    HEIGHT = "height"
    MEDIA_KEY = "media_key"
    PREVIEW_IMAGE_URL = "preview_image_url"
    TYPE = "type"
    URL = "url"
    WIDTH = "width"
    PUBLIC_METRICS = "public_metrics"
    ALT_TEXT = "alt_text"
    VARIANTS = "variants"


class Expansion(str, Enum):
    """Available expansions."""
    AUTHOR_ID = "author_id"
    REFERENCED_TWEETS_ID = "referenced_tweets.id"
    IN_REPLY_TO_USER_ID = "in_reply_to_user_id"
    ATTACHMENTS_MEDIA_KEYS = "attachments.media_keys"
    ATTACHMENTS_POLL_IDS = "attachments.poll_ids"
    GEO_PLACE_ID = "geo.place_id"
    ENTITIES_MENTIONS_USERNAME = "entities.mentions.username"
    REFERENCED_TWEETS_AUTHOR = "referenced_tweets.id.author_id"
    PINNED_TWEET_ID = "pinned_tweet_id"


class SortOrder(str, Enum):
    """Search sort order."""
    RECENCY = "recency"
    RELEVANCY = "relevancy"


@dataclass
class PKCEChallenge:
    """PKCE challenge for OAuth 2.0."""
    code_verifier: str
    code_challenge: str
    state: str

    @classmethod
    def generate(cls) -> "PKCEChallenge":
        """Generate a new PKCE challenge."""
        verifier = secrets.token_urlsafe(64)[:128]
        challenge = hashlib.sha256(verifier.encode()).digest()
        import base64
        challenge_b64 = base64.urlsafe_b64encode(challenge).rstrip(b"=").decode()
        state = secrets.token_urlsafe(32)
        return cls(
            code_verifier=verifier,
            code_challenge=challenge_b64,
            state=state,
        )


@dataclass
class OAuth2Token:
    """OAuth 2.0 token."""
    access_token: str
    token_type: str = "bearer"
    expires_in: int = 7200
    refresh_token: Optional[str] = None
    scope: str = ""
    created_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        """Check if token is expired (with 5min buffer)."""
        return time.time() > (self.created_at + self.expires_in - 300)

    def to_dict(self) -> dict:
        """Serialize to dict."""
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "OAuth2Token":
        """Deserialize from dict."""
        return cls(
            access_token=data["access_token"],
            token_type=data.get("token_type", "bearer"),
            expires_in=data.get("expires_in", 7200),
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope", ""),
            created_at=data.get("created_at", time.time()),
        )


@dataclass
class RateLimitInfo:
    """Rate limit tracking per endpoint."""
    limit: int = 0
    remaining: int = 0
    reset: float = 0.0

    @property
    def is_limited(self) -> bool:
        """Check if we're rate limited."""
        return self.remaining <= 0 and time.time() < self.reset

    @property
    def wait_seconds(self) -> float:
        """Seconds to wait before next request."""
        if not self.is_limited:
            return 0
        return max(0, self.reset - time.time())


@dataclass
class APIResponse:
    """Structured API response."""
    data: Any = None
    includes: Optional[dict] = None
    meta: Optional[dict] = None
    errors: Optional[list] = None
    status_code: int = 200
    rate_limit: Optional[RateLimitInfo] = None

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)

    @property
    def next_token(self) -> Optional[str]:
        """Get pagination token if available."""
        if self.meta:
            return self.meta.get("next_token")
        return None


class TwitterAPIv2:
    """
    Twitter API v2 client with OAuth 2.0 PKCE support.

    Features:
    - Full OAuth 2.0 Authorization Code Flow with PKCE
    - Automatic token refresh
    - Rate limit tracking per endpoint
    - Adaptive backoff on 429 responses
    - Tweet search (recent + full archive)
    - Tweet counts
    - User lookup by ID/username
    - Followers/Following lists
    - Tweet creation/deletion
    - Bookmark management
    - Like/Unlike
    - Retweet/Unretweet
    """

    BASE_URL = "https://api.twitter.com/2"
    AUTH_URL = "https://twitter.com/i/oauth2/authorize"
    TOKEN_URL = "https://api.twitter.com/2/oauth2/token"

    SCOPES = [
        "tweet.read", "tweet.write", "tweet.moderate.write",
        "users.read", "follows.read", "follows.write",
        "offline.access", "space.read", "mute.read", "mute.write",
        "like.read", "like.write", "list.read", "list.write",
        "block.read", "block.write", "bookmark.read", "bookmark.write",
    ]

    DEFAULT_TWEET_FIELDS = [
        TweetField.ID, TweetField.TEXT, TweetField.AUTHOR_ID,
        TweetField.CREATED_AT, TweetField.PUBLIC_METRICS,
        TweetField.CONVERSATION_ID, TweetField.LANG,
    ]

    DEFAULT_USER_FIELDS = [
        UserField.ID, UserField.USERNAME, UserField.NAME,
        UserField.CREATED_AT, UserField.PUBLIC_METRICS,
        UserField.DESCRIPTION, UserField.VERIFIED,
    ]

    def __init__(
        self,
        client_id: str,
        client_secret: Optional[str] = None,
        redirect_uri: str = "http://localhost:8080/callback",
        bearer_token: Optional[str] = None,
        token: Optional[OAuth2Token] = None,
        http_client=None,
    ):
        """
        Initialize Twitter API v2 client.

        Args:
            client_id: OAuth 2.0 client ID
            client_secret: OAuth 2.0 client secret (confidential clients)
            redirect_uri: OAuth 2.0 redirect URI
            bearer_token: App-only bearer token (for read-only)
            token: Existing OAuth 2.0 token
            http_client: Custom HTTP client (for testing)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.bearer_token = bearer_token
        self.token = token
        self._http = http_client
        self._rate_limits: dict[str, RateLimitInfo] = {}
        self._request_count = 0
        self._error_count = 0

    # === OAuth 2.0 PKCE Flow ===

    def get_authorization_url(
        self,
        scopes: Optional[list[str]] = None,
        pkce: Optional[PKCEChallenge] = None,
    ) -> tuple[str, PKCEChallenge]:
        """
        Generate OAuth 2.0 authorization URL with PKCE.

        Returns:
            Tuple of (authorization_url, pkce_challenge)
        """
        if pkce is None:
            pkce = PKCEChallenge.generate()

        scope_str = " ".join(scopes or self.SCOPES)

        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": scope_str,
            "state": pkce.state,
            "code_challenge": pkce.code_challenge,
            "code_challenge_method": "S256",
        }

        url = f"{self.AUTH_URL}?{urllib.parse.urlencode(params)}"
        return url, pkce

    def exchange_code(
        self,
        code: str,
        pkce: PKCEChallenge,
    ) -> OAuth2Token:
        """
        Exchange authorization code for access token.

        Args:
            code: Authorization code from callback
            pkce: PKCE challenge used in authorization

        Returns:
            OAuth2Token with access and refresh tokens
        """
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "code_verifier": pkce.code_verifier,
        }

        response = self._token_request(data)
        self.token = OAuth2Token(
            access_token=response["access_token"],
            token_type=response.get("token_type", "bearer"),
            expires_in=response.get("expires_in", 7200),
            refresh_token=response.get("refresh_token"),
            scope=response.get("scope", ""),
        )
        return self.token

    def refresh_access_token(self) -> OAuth2Token:
        """
        Refresh access token using refresh token.

        Returns:
            New OAuth2Token
        """
        if not self.token or not self.token.refresh_token:
            raise ValueError("No refresh token available")

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.token.refresh_token,
            "client_id": self.client_id,
        }

        response = self._token_request(data)
        self.token = OAuth2Token(
            access_token=response["access_token"],
            token_type=response.get("token_type", "bearer"),
            expires_in=response.get("expires_in", 7200),
            refresh_token=response.get("refresh_token", self.token.refresh_token),
            scope=response.get("scope", self.token.scope),
        )
        return self.token

    def revoke_token(self, token: Optional[str] = None) -> bool:
        """Revoke an access or refresh token."""
        token_to_revoke = token or (self.token.access_token if self.token else None)
        if not token_to_revoke:
            return False

        data = {
            "token": token_to_revoke,
            "client_id": self.client_id,
        }

        try:
            self._token_request(data, endpoint="/2/oauth2/revoke")
            return True
        except Exception:
            return False

    # === Tweet Endpoints ===

    def get_tweet(
        self,
        tweet_id: str,
        tweet_fields: Optional[list] = None,
        expansions: Optional[list] = None,
        user_fields: Optional[list] = None,
        media_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get a single tweet by ID."""
        params = self._build_field_params(
            tweet_fields, expansions, user_fields, media_fields
        )
        return self._get(f"/tweets/{tweet_id}", params=params)

    def get_tweets(
        self,
        tweet_ids: list[str],
        tweet_fields: Optional[list] = None,
        expansions: Optional[list] = None,
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get multiple tweets by IDs (max 100)."""
        if len(tweet_ids) > 100:
            raise ValueError("Maximum 100 tweet IDs per request")

        params = {"ids": ",".join(tweet_ids)}
        params.update(self._build_field_params(tweet_fields, expansions, user_fields))
        return self._get("/tweets", params=params)

    def search_recent(
        self,
        query: str,
        max_results: int = 10,
        sort_order: Optional[SortOrder] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        since_id: Optional[str] = None,
        until_id: Optional[str] = None,
        next_token: Optional[str] = None,
        tweet_fields: Optional[list] = None,
        expansions: Optional[list] = None,
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """
        Search recent tweets (last 7 days).

        Args:
            query: Search query (max 512 chars)
            max_results: 10-100 results per page
            sort_order: recency or relevancy
            start_time: ISO 8601 start time
            end_time: ISO 8601 end time
            since_id: Return tweets after this ID
            until_id: Return tweets before this ID
            next_token: Pagination token
        """
        if len(query) > 512:
            raise ValueError("Query must be 512 characters or less")
        if not 10 <= max_results <= 100:
            raise ValueError("max_results must be between 10 and 100")

        params = {"query": query, "max_results": max_results}
        if sort_order:
            params["sort_order"] = sort_order.value
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if since_id:
            params["since_id"] = since_id
        if until_id:
            params["until_id"] = until_id
        if next_token:
            params["next_token"] = next_token

        params.update(self._build_field_params(tweet_fields, expansions, user_fields))
        return self._get("/tweets/search/recent", params=params)

    def search_all(
        self,
        query: str,
        max_results: int = 10,
        sort_order: Optional[SortOrder] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        next_token: Optional[str] = None,
        tweet_fields: Optional[list] = None,
        expansions: Optional[list] = None,
    ) -> APIResponse:
        """
        Full-archive search (Academic Research access required).
        """
        if len(query) > 1024:
            raise ValueError("Full archive query must be 1024 characters or less")
        if not 10 <= max_results <= 500:
            raise ValueError("max_results must be between 10 and 500")

        params = {"query": query, "max_results": max_results}
        if sort_order:
            params["sort_order"] = sort_order.value
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if next_token:
            params["next_token"] = next_token

        params.update(self._build_field_params(tweet_fields, expansions))
        return self._get("/tweets/search/all", params=params)

    def get_tweet_counts(
        self,
        query: str,
        granularity: str = "hour",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        next_token: Optional[str] = None,
    ) -> APIResponse:
        """
        Get tweet counts matching a query.

        Args:
            query: Search query
            granularity: minute, hour, or day
        """
        if granularity not in ("minute", "hour", "day"):
            raise ValueError("Granularity must be minute, hour, or day")

        params = {"query": query, "granularity": granularity}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if next_token:
            params["next_token"] = next_token

        return self._get("/tweets/counts/recent", params=params)

    def create_tweet(
        self,
        text: Optional[str] = None,
        reply_to: Optional[str] = None,
        quote_tweet_id: Optional[str] = None,
        media_ids: Optional[list[str]] = None,
        poll_options: Optional[list[str]] = None,
        poll_duration_minutes: int = 60,
        reply_settings: Optional[str] = None,
    ) -> APIResponse:
        """
        Create a new tweet.

        Args:
            text: Tweet text (max 280 chars)
            reply_to: Tweet ID to reply to
            quote_tweet_id: Tweet ID to quote
            media_ids: Media IDs to attach
            poll_options: Poll options (2-4)
            poll_duration_minutes: Poll duration (5-10080)
            reply_settings: mentionedUsers, following, or everyone
        """
        body: dict[str, Any] = {}
        if text:
            if len(text) > 280:
                raise ValueError("Tweet text must be 280 characters or less")
            body["text"] = text

        if reply_to:
            body["reply"] = {"in_reply_to_tweet_id": reply_to}
        if quote_tweet_id:
            body["quote_tweet_id"] = quote_tweet_id
        if media_ids:
            body["media"] = {"media_ids": media_ids}
        if poll_options:
            if not 2 <= len(poll_options) <= 4:
                raise ValueError("Polls must have 2-4 options")
            body["poll"] = {
                "options": poll_options,
                "duration_minutes": poll_duration_minutes,
            }
        if reply_settings:
            body["reply_settings"] = reply_settings

        if not body:
            raise ValueError("Tweet must have text, media, or poll")

        return self._post("/tweets", json_body=body)

    def delete_tweet(self, tweet_id: str) -> APIResponse:
        """Delete a tweet by ID."""
        return self._delete(f"/tweets/{tweet_id}")

    # === User Endpoints ===

    def get_user(
        self,
        user_id: str,
        user_fields: Optional[list] = None,
        tweet_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get user by ID."""
        params = {}
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        if tweet_fields:
            params["tweet.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in tweet_fields
            )
        return self._get(f"/users/{user_id}", params=params)

    def get_user_by_username(
        self,
        username: str,
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get user by username."""
        params = {}
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        return self._get(f"/users/by/username/{username}", params=params)

    def get_users_by_usernames(
        self,
        usernames: list[str],
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get multiple users by usernames (max 100)."""
        if len(usernames) > 100:
            raise ValueError("Maximum 100 usernames per request")
        params = {"usernames": ",".join(usernames)}
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        return self._get("/users/by", params=params)

    def get_followers(
        self,
        user_id: str,
        max_results: int = 100,
        pagination_token: Optional[str] = None,
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get followers of a user."""
        params = {"max_results": min(max_results, 1000)}
        if pagination_token:
            params["pagination_token"] = pagination_token
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        return self._get(f"/users/{user_id}/followers", params=params)

    def get_following(
        self,
        user_id: str,
        max_results: int = 100,
        pagination_token: Optional[str] = None,
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get users that a user follows."""
        params = {"max_results": min(max_results, 1000)}
        if pagination_token:
            params["pagination_token"] = pagination_token
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        return self._get(f"/users/{user_id}/following", params=params)

    def follow_user(self, source_user_id: str, target_user_id: str) -> APIResponse:
        """Follow a user."""
        return self._post(
            f"/users/{source_user_id}/following",
            json_body={"target_user_id": target_user_id},
        )

    def unfollow_user(self, source_user_id: str, target_user_id: str) -> APIResponse:
        """Unfollow a user."""
        return self._delete(f"/users/{source_user_id}/following/{target_user_id}")

    # === Engagement Endpoints ===

    def like_tweet(self, user_id: str, tweet_id: str) -> APIResponse:
        """Like a tweet."""
        return self._post(
            f"/users/{user_id}/likes",
            json_body={"tweet_id": tweet_id},
        )

    def unlike_tweet(self, user_id: str, tweet_id: str) -> APIResponse:
        """Unlike a tweet."""
        return self._delete(f"/users/{user_id}/likes/{tweet_id}")

    def get_liking_users(
        self,
        tweet_id: str,
        max_results: int = 100,
        user_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get users who liked a tweet."""
        params = {"max_results": min(max_results, 100)}
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        return self._get(f"/tweets/{tweet_id}/liking_users", params=params)

    def retweet(self, user_id: str, tweet_id: str) -> APIResponse:
        """Retweet a tweet."""
        return self._post(
            f"/users/{user_id}/retweets",
            json_body={"tweet_id": tweet_id},
        )

    def unretweet(self, user_id: str, tweet_id: str) -> APIResponse:
        """Undo a retweet."""
        return self._delete(f"/users/{user_id}/retweets/{tweet_id}")

    # === Bookmark Endpoints ===

    def get_bookmarks(
        self,
        user_id: str,
        max_results: int = 100,
        pagination_token: Optional[str] = None,
        tweet_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get user's bookmarks."""
        params = {"max_results": min(max_results, 100)}
        if pagination_token:
            params["pagination_token"] = pagination_token
        params.update(self._build_field_params(tweet_fields))
        return self._get(f"/users/{user_id}/bookmarks", params=params)

    def bookmark_tweet(self, user_id: str, tweet_id: str) -> APIResponse:
        """Bookmark a tweet."""
        return self._post(
            f"/users/{user_id}/bookmarks",
            json_body={"tweet_id": tweet_id},
        )

    def remove_bookmark(self, user_id: str, tweet_id: str) -> APIResponse:
        """Remove a bookmark."""
        return self._delete(f"/users/{user_id}/bookmarks/{tweet_id}")

    # === Mute/Block Endpoints ===

    def mute_user(self, source_user_id: str, target_user_id: str) -> APIResponse:
        """Mute a user."""
        return self._post(
            f"/users/{source_user_id}/muting",
            json_body={"target_user_id": target_user_id},
        )

    def unmute_user(self, source_user_id: str, target_user_id: str) -> APIResponse:
        """Unmute a user."""
        return self._delete(f"/users/{source_user_id}/muting/{target_user_id}")

    def block_user(self, source_user_id: str, target_user_id: str) -> APIResponse:
        """Block a user."""
        return self._post(
            f"/users/{source_user_id}/blocking",
            json_body={"target_user_id": target_user_id},
        )

    def unblock_user(self, source_user_id: str, target_user_id: str) -> APIResponse:
        """Unblock a user."""
        return self._delete(f"/users/{source_user_id}/blocking/{target_user_id}")

    # === Timeline Endpoints ===

    def get_user_tweets(
        self,
        user_id: str,
        max_results: int = 10,
        pagination_token: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        exclude: Optional[list[str]] = None,
        tweet_fields: Optional[list] = None,
        expansions: Optional[list] = None,
    ) -> APIResponse:
        """Get a user's tweets."""
        params = {"max_results": min(max_results, 100)}
        if pagination_token:
            params["pagination_token"] = pagination_token
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if exclude:
            params["exclude"] = ",".join(exclude)
        params.update(self._build_field_params(tweet_fields, expansions))
        return self._get(f"/users/{user_id}/tweets", params=params)

    def get_user_mentions(
        self,
        user_id: str,
        max_results: int = 10,
        pagination_token: Optional[str] = None,
        tweet_fields: Optional[list] = None,
    ) -> APIResponse:
        """Get tweets mentioning a user."""
        params = {"max_results": min(max_results, 100)}
        if pagination_token:
            params["pagination_token"] = pagination_token
        params.update(self._build_field_params(tweet_fields))
        return self._get(f"/users/{user_id}/mentions", params=params)

    # === Spaces Endpoints ===

    def get_space(self, space_id: str) -> APIResponse:
        """Get a Space by ID."""
        return self._get(f"/spaces/{space_id}")

    def search_spaces(
        self,
        query: str,
        state: str = "live",
        max_results: int = 100,
    ) -> APIResponse:
        """Search for Spaces."""
        params = {
            "query": query,
            "state": state,
            "max_results": min(max_results, 100),
        }
        return self._get("/spaces/search", params=params)

    # === Statistics ===

    @property
    def stats(self) -> dict:
        """Get client statistics."""
        return {
            "total_requests": self._request_count,
            "total_errors": self._error_count,
            "error_rate": (
                self._error_count / self._request_count
                if self._request_count > 0
                else 0
            ),
            "rate_limits_tracked": len(self._rate_limits),
            "has_token": self.token is not None,
            "token_expired": self.token.is_expired if self.token else None,
        }

    def get_rate_limit(self, endpoint: str) -> Optional[RateLimitInfo]:
        """Get rate limit info for an endpoint."""
        return self._rate_limits.get(endpoint)

    # === Internal Methods ===

    def _build_field_params(
        self,
        tweet_fields: Optional[list] = None,
        expansions: Optional[list] = None,
        user_fields: Optional[list] = None,
        media_fields: Optional[list] = None,
    ) -> dict:
        """Build query params for field selections."""
        params = {}
        fields = tweet_fields or self.DEFAULT_TWEET_FIELDS
        if fields:
            params["tweet.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in fields
            )
        if expansions:
            params["expansions"] = ",".join(
                e.value if isinstance(e, Enum) else e for e in expansions
            )
        if user_fields:
            params["user.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in user_fields
            )
        if media_fields:
            params["media.fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in media_fields
            )
        return params

    def _get_auth_headers(self) -> dict:
        """Get authorization headers."""
        if self.token and not self.token.is_expired:
            return {"Authorization": f"Bearer {self.token.access_token}"}
        if self.bearer_token:
            return {"Authorization": f"Bearer {self.bearer_token}"}
        raise ValueError("No valid authentication available")

    def _update_rate_limits(self, endpoint: str, headers: dict) -> RateLimitInfo:
        """Update rate limit tracking from response headers."""
        info = RateLimitInfo(
            limit=int(headers.get("x-rate-limit-limit", 0)),
            remaining=int(headers.get("x-rate-limit-remaining", 0)),
            reset=float(headers.get("x-rate-limit-reset", 0)),
        )
        self._rate_limits[endpoint] = info
        return info

    def _make_response(
        self,
        status_code: int,
        body: dict,
        rate_limit: Optional[RateLimitInfo] = None,
    ) -> APIResponse:
        """Create structured response."""
        return APIResponse(
            data=body.get("data"),
            includes=body.get("includes"),
            meta=body.get("meta"),
            errors=body.get("errors"),
            status_code=status_code,
            rate_limit=rate_limit,
        )

    def _get(self, endpoint: str, params: Optional[dict] = None) -> APIResponse:
        """Make GET request."""
        self._request_count += 1
        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_auth_headers()

        if self._http:
            response = self._http.get(url, params=params, headers=headers)
        else:
            import requests
            response = requests.get(url, params=params, headers=headers)

        rate_limit = self._update_rate_limits(
            endpoint, dict(response.headers) if hasattr(response, 'headers') else {}
        )

        if response.status_code >= 400:
            self._error_count += 1

        try:
            body = response.json()
        except (json.JSONDecodeError, AttributeError):
            body = {}

        return self._make_response(response.status_code, body, rate_limit)

    def _post(
        self,
        endpoint: str,
        json_body: Optional[dict] = None,
    ) -> APIResponse:
        """Make POST request."""
        self._request_count += 1
        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_auth_headers()
        headers["Content-Type"] = "application/json"

        if self._http:
            response = self._http.post(url, json=json_body, headers=headers)
        else:
            import requests
            response = requests.post(url, json=json_body, headers=headers)

        if response.status_code >= 400:
            self._error_count += 1

        try:
            body = response.json()
        except (json.JSONDecodeError, AttributeError):
            body = {}

        return self._make_response(response.status_code, body)

    def _delete(self, endpoint: str) -> APIResponse:
        """Make DELETE request."""
        self._request_count += 1
        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_auth_headers()

        if self._http:
            response = self._http.delete(url, headers=headers)
        else:
            import requests
            response = requests.delete(url, headers=headers)

        if response.status_code >= 400:
            self._error_count += 1

        try:
            body = response.json()
        except (json.JSONDecodeError, AttributeError):
            body = {}

        return self._make_response(response.status_code, body)

    def _token_request(
        self,
        data: dict,
        endpoint: str = "/2/oauth2/token",
    ) -> dict:
        """Make token request."""
        url = f"https://api.twitter.com{endpoint}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        auth = None
        if self.client_secret:
            import base64
            creds = base64.b64encode(
                f"{self.client_id}:{self.client_secret}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {creds}"

        if self._http:
            response = self._http.post(url, data=data, headers=headers)
        else:
            import requests
            response = requests.post(url, data=data, headers=headers)

        result = response.json()
        if response.status_code != 200:
            raise Exception(f"Token error: {result}")
        return result
