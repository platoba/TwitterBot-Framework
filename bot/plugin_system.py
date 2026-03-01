"""
Plugin system for TwitterBot Framework.

Provides a modular plugin architecture for extending bot functionality:
- Plugin discovery and loading
- Lifecycle management (init → start → stop → unload)
- Event system with publish/subscribe
- Hook system for intercepting core operations
- Plugin dependency resolution
- Configuration per plugin
- Health monitoring
"""

import importlib
import inspect
import json
import os
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class PluginState(str, Enum):
    """Plugin lifecycle states."""
    DISCOVERED = "discovered"
    LOADED = "loaded"
    INITIALIZED = "initialized"
    STARTED = "started"
    STOPPED = "stopped"
    ERROR = "error"
    UNLOADED = "unloaded"


class HookPriority(int, Enum):
    """Hook execution priority (lower = earlier)."""
    HIGHEST = 0
    HIGH = 25
    NORMAL = 50
    LOW = 75
    LOWEST = 100


class EventPriority(int, Enum):
    """Event handler priority."""
    FIRST = 0
    NORMAL = 50
    LAST = 100


@dataclass
class PluginMeta:
    """Plugin metadata."""
    name: str
    version: str = "1.0.0"
    description: str = ""
    author: str = ""
    dependencies: list[str] = field(default_factory=list)
    config_schema: dict = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class PluginInfo:
    """Runtime plugin information."""
    meta: PluginMeta
    state: PluginState = PluginState.DISCOVERED
    instance: Any = None
    module: Any = None
    config: dict = field(default_factory=dict)
    error: Optional[str] = None
    loaded_at: Optional[float] = None
    started_at: Optional[float] = None
    event_count: int = 0
    hook_count: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.meta.name,
            "version": self.meta.version,
            "description": self.meta.description,
            "author": self.meta.author,
            "state": self.state.value,
            "dependencies": self.meta.dependencies,
            "tags": self.meta.tags,
            "error": self.error,
            "loaded_at": self.loaded_at,
            "started_at": self.started_at,
            "event_count": self.event_count,
            "hook_count": self.hook_count,
            "uptime": time.time() - self.started_at if self.started_at else 0,
        }


@dataclass
class EventHandler:
    """Registered event handler."""
    plugin_name: str
    event_name: str
    handler: Callable
    priority: EventPriority = EventPriority.NORMAL
    filters: Optional[dict] = None

    def matches(self, event_data: dict) -> bool:
        """Check if handler filters match event data."""
        if not self.filters:
            return True
        return all(
            event_data.get(k) == v for k, v in self.filters.items()
        )


@dataclass
class HookHandler:
    """Registered hook handler."""
    plugin_name: str
    hook_name: str
    handler: Callable
    priority: HookPriority = HookPriority.NORMAL


class PluginBase:
    """
    Base class for TwitterBot plugins.

    Override lifecycle methods and use decorators for events/hooks.
    """

    # Override in subclass
    META = PluginMeta(name="base_plugin")

    def __init__(self, bot=None, config: Optional[dict] = None):
        self.bot = bot
        self.config = config or {}
        self._event_handlers: list[tuple[str, Callable, EventPriority]] = []
        self._hook_handlers: list[tuple[str, Callable, HookPriority]] = []

    async def on_init(self):
        """Called when plugin is initialized. Setup resources here."""
        pass

    async def on_start(self):
        """Called when plugin is started. Begin background tasks here."""
        pass

    async def on_stop(self):
        """Called when plugin is stopped. Cleanup resources here."""
        pass

    async def on_unload(self):
        """Called when plugin is unloaded. Final cleanup."""
        pass

    def on_init_sync(self):
        """Sync version of on_init for non-async plugins."""
        pass

    def on_start_sync(self):
        """Sync version of on_start."""
        pass

    def on_stop_sync(self):
        """Sync version of on_stop."""
        pass

    def on_unload_sync(self):
        """Sync version of on_unload."""
        pass

    def register_event(
        self,
        event_name: str,
        handler: Callable,
        priority: EventPriority = EventPriority.NORMAL,
    ):
        """Register an event handler."""
        self._event_handlers.append((event_name, handler, priority))

    def register_hook(
        self,
        hook_name: str,
        handler: Callable,
        priority: HookPriority = HookPriority.NORMAL,
    ):
        """Register a hook handler."""
        self._hook_handlers.append((hook_name, handler, priority))


def event_handler(
    event_name: str,
    priority: EventPriority = EventPriority.NORMAL,
    **filters,
):
    """Decorator to mark a method as an event handler."""
    def decorator(func):
        func._event_name = event_name
        func._event_priority = priority
        func._event_filters = filters
        return func
    return decorator


def hook_handler(
    hook_name: str,
    priority: HookPriority = HookPriority.NORMAL,
):
    """Decorator to mark a method as a hook handler."""
    def decorator(func):
        func._hook_name = hook_name
        func._hook_priority = priority
        return func
    return decorator


class PluginManager:
    """
    Manages plugin lifecycle, events, and hooks.

    Features:
    - Plugin discovery from directory or explicit registration
    - Dependency resolution with cycle detection
    - Event publish/subscribe with priority ordering
    - Hook chain with data transformation
    - Per-plugin configuration
    - Health monitoring and error isolation
    """

    def __init__(self, bot=None, plugin_dir: Optional[str] = None):
        self.bot = bot
        self.plugin_dir = plugin_dir
        self._plugins: dict[str, PluginInfo] = {}
        self._event_handlers: dict[str, list[EventHandler]] = {}
        self._hook_handlers: dict[str, list[HookHandler]] = {}
        self._event_log: list[dict] = []
        self._max_log_size = 1000

    @property
    def plugins(self) -> dict[str, PluginInfo]:
        """Get all registered plugins."""
        return dict(self._plugins)

    @property
    def active_plugins(self) -> list[str]:
        """Get names of active (started) plugins."""
        return [
            name for name, info in self._plugins.items()
            if info.state == PluginState.STARTED
        ]

    def register_plugin(
        self,
        plugin_class: type,
        config: Optional[dict] = None,
    ) -> PluginInfo:
        """
        Register a plugin class.

        Args:
            plugin_class: Plugin class (subclass of PluginBase)
            config: Plugin configuration

        Returns:
            PluginInfo for the registered plugin
        """
        meta = getattr(plugin_class, "META", None)
        if not meta:
            raise ValueError(f"Plugin {plugin_class.__name__} missing META attribute")

        if meta.name in self._plugins:
            raise ValueError(f"Plugin '{meta.name}' already registered")

        info = PluginInfo(
            meta=meta,
            state=PluginState.DISCOVERED,
            config=config or {},
        )

        # Store the class for later instantiation
        info.module = plugin_class
        self._plugins[meta.name] = info
        return info

    def discover_plugins(self, directory: Optional[str] = None) -> list[str]:
        """
        Discover plugins from a directory.

        Each plugin should be a Python file or package with a class
        that subclasses PluginBase.

        Returns:
            List of discovered plugin names
        """
        search_dir = directory or self.plugin_dir
        if not search_dir or not os.path.isdir(search_dir):
            return []

        discovered = []
        for item in os.listdir(search_dir):
            path = os.path.join(search_dir, item)

            # Python file
            if item.endswith(".py") and not item.startswith("_"):
                try:
                    name = item[:-3]
                    spec = importlib.util.spec_from_file_location(
                        f"plugins.{name}", path
                    )
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)

                        # Find PluginBase subclasses
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if (
                                inspect.isclass(attr)
                                and issubclass(attr, PluginBase)
                                and attr is not PluginBase
                            ):
                                self.register_plugin(attr)
                                discovered.append(attr.META.name)
                except Exception as e:
                    # Log but don't fail
                    self._event_log.append({
                        "type": "discovery_error",
                        "file": item,
                        "error": str(e),
                        "timestamp": time.time(),
                    })

            # Package directory
            elif os.path.isdir(path):
                init_path = os.path.join(path, "__init__.py")
                if os.path.exists(init_path):
                    try:
                        spec = importlib.util.spec_from_file_location(
                            f"plugins.{item}", init_path
                        )
                        if spec and spec.loader:
                            module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(module)

                            for attr_name in dir(module):
                                attr = getattr(module, attr_name)
                                if (
                                    inspect.isclass(attr)
                                    and issubclass(attr, PluginBase)
                                    and attr is not PluginBase
                                ):
                                    self.register_plugin(attr)
                                    discovered.append(attr.META.name)
                    except Exception as e:
                        self._event_log.append({
                            "type": "discovery_error",
                            "package": item,
                            "error": str(e),
                            "timestamp": time.time(),
                        })

        return discovered

    def resolve_dependencies(self) -> list[str]:
        """
        Resolve plugin load order based on dependencies.
        Raises ValueError on circular dependencies.

        Returns:
            Ordered list of plugin names
        """
        # Build dependency graph
        graph: dict[str, set[str]] = {}
        for name, info in self._plugins.items():
            graph[name] = set(info.meta.dependencies)

        # Topological sort (Kahn's algorithm)
        in_degree = {name: 0 for name in graph}
        for name, deps in graph.items():
            for dep in deps:
                if dep in in_degree:
                    in_degree[dep] = in_degree.get(dep, 0)

        # Calculate actual in-degrees from reverse deps
        reverse = {name: set() for name in graph}
        for name, deps in graph.items():
            for dep in deps:
                if dep in reverse:
                    reverse[dep].add(name)

        in_degree = {name: len(deps) for name, deps in graph.items()}
        # Re-count: in_degree[x] = how many plugins x depends on that exist
        in_degree = {}
        for name, deps in graph.items():
            existing_deps = deps & set(graph.keys())
            in_degree[name] = len(existing_deps)

        queue = [name for name, deg in in_degree.items() if deg == 0]
        order = []

        while queue:
            node = queue.pop(0)
            order.append(node)
            # Find plugins that depend on this node
            for name, deps in graph.items():
                if node in deps:
                    in_degree[name] -= 1
                    if in_degree[name] == 0:
                        queue.append(name)

        if len(order) != len(graph):
            missing = set(graph.keys()) - set(order)
            raise ValueError(f"Circular dependency detected among: {missing}")

        return order

    def load_plugin(self, name: str) -> PluginInfo:
        """
        Load and instantiate a plugin.

        Args:
            name: Plugin name

        Returns:
            Updated PluginInfo
        """
        if name not in self._plugins:
            raise ValueError(f"Plugin '{name}' not registered")

        info = self._plugins[name]

        # Check dependencies
        for dep in info.meta.dependencies:
            if dep in self._plugins:
                dep_info = self._plugins[dep]
                if dep_info.state not in (
                    PluginState.LOADED,
                    PluginState.INITIALIZED,
                    PluginState.STARTED,
                ):
                    raise ValueError(
                        f"Dependency '{dep}' not loaded for plugin '{name}'"
                    )

        try:
            # Instantiate
            plugin_class = info.module
            instance = plugin_class(bot=self.bot, config=info.config)
            info.instance = instance
            info.state = PluginState.LOADED
            info.loaded_at = time.time()

            # Discover decorated handlers
            self._discover_handlers(name, instance)

        except Exception as e:
            info.state = PluginState.ERROR
            info.error = f"Load error: {str(e)}\n{traceback.format_exc()}"

        return info

    def init_plugin(self, name: str) -> PluginInfo:
        """Initialize a loaded plugin."""
        info = self._plugins.get(name)
        if not info:
            raise ValueError(f"Plugin '{name}' not registered")
        if info.state != PluginState.LOADED:
            raise ValueError(f"Plugin '{name}' not in LOADED state (is {info.state.value})")

        try:
            instance = info.instance
            # Try async first, fall back to sync
            if inspect.iscoroutinefunction(instance.on_init):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # We're in an async context
                        pass
                    else:
                        loop.run_until_complete(instance.on_init())
                except RuntimeError:
                    asyncio.run(instance.on_init())
            else:
                instance.on_init_sync()

            info.state = PluginState.INITIALIZED
        except Exception as e:
            info.state = PluginState.ERROR
            info.error = f"Init error: {str(e)}"

        return info

    def start_plugin(self, name: str) -> PluginInfo:
        """Start an initialized plugin."""
        info = self._plugins.get(name)
        if not info:
            raise ValueError(f"Plugin '{name}' not registered")
        if info.state != PluginState.INITIALIZED:
            raise ValueError(f"Plugin '{name}' not in INITIALIZED state (is {info.state.value})")

        try:
            instance = info.instance
            if inspect.iscoroutinefunction(instance.on_start):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if not loop.is_running():
                        loop.run_until_complete(instance.on_start())
                except RuntimeError:
                    asyncio.run(instance.on_start())
            else:
                instance.on_start_sync()

            info.state = PluginState.STARTED
            info.started_at = time.time()

            self._emit_log("plugin_started", {"plugin": name})
        except Exception as e:
            info.state = PluginState.ERROR
            info.error = f"Start error: {str(e)}"

        return info

    def stop_plugin(self, name: str) -> PluginInfo:
        """Stop a started plugin."""
        info = self._plugins.get(name)
        if not info:
            raise ValueError(f"Plugin '{name}' not registered")
        if info.state != PluginState.STARTED:
            return info

        try:
            instance = info.instance
            if inspect.iscoroutinefunction(instance.on_stop):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if not loop.is_running():
                        loop.run_until_complete(instance.on_stop())
                except RuntimeError:
                    asyncio.run(instance.on_stop())
            else:
                instance.on_stop_sync()

            info.state = PluginState.STOPPED
            self._emit_log("plugin_stopped", {"plugin": name})
        except Exception as e:
            info.state = PluginState.ERROR
            info.error = f"Stop error: {str(e)}"

        return info

    def unload_plugin(self, name: str) -> bool:
        """Unload a plugin completely."""
        info = self._plugins.get(name)
        if not info:
            return False

        # Stop if running
        if info.state == PluginState.STARTED:
            self.stop_plugin(name)

        # Cleanup
        if info.instance:
            try:
                if inspect.iscoroutinefunction(info.instance.on_unload):
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if not loop.is_running():
                            loop.run_until_complete(info.instance.on_unload())
                    except RuntimeError:
                        asyncio.run(info.instance.on_unload())
                else:
                    info.instance.on_unload_sync()
            except Exception:
                pass

        # Remove handlers
        self._remove_handlers(name)

        info.state = PluginState.UNLOADED
        info.instance = None
        return True

    def load_all(self, configs: Optional[dict[str, dict]] = None):
        """Load all registered plugins in dependency order."""
        configs = configs or {}

        order = self.resolve_dependencies()

        for name in order:
            if name in configs:
                self._plugins[name].config = configs[name]
            self.load_plugin(name)

    def start_all(self):
        """Initialize and start all loaded plugins."""
        order = self.resolve_dependencies()

        for name in order:
            info = self._plugins.get(name)
            if info and info.state == PluginState.LOADED:
                self.init_plugin(name)
            if info and info.state == PluginState.INITIALIZED:
                self.start_plugin(name)

    def stop_all(self):
        """Stop all started plugins (reverse order)."""
        order = self.resolve_dependencies()
        for name in reversed(order):
            info = self._plugins.get(name)
            if info and info.state == PluginState.STARTED:
                self.stop_plugin(name)

    # === Event System ===

    def emit(self, event_name: str, data: Optional[dict] = None) -> list:
        """
        Emit an event to all registered handlers.

        Args:
            event_name: Event name
            data: Event data

        Returns:
            List of handler results
        """
        data = data or {}
        results = []

        handlers = self._event_handlers.get(event_name, [])
        # Sort by priority
        handlers.sort(key=lambda h: h.priority.value)

        for handler in handlers:
            # Check if plugin is active
            info = self._plugins.get(handler.plugin_name)
            if not info or info.state != PluginState.STARTED:
                continue

            # Check filters
            if not handler.matches(data):
                continue

            try:
                result = handler.handler(data)
                results.append(result)
                info.event_count += 1
            except Exception as e:
                self._emit_log("event_error", {
                    "event": event_name,
                    "plugin": handler.plugin_name,
                    "error": str(e),
                })

        self._emit_log("event", {"name": event_name, "handlers": len(results)})
        return results

    def subscribe(
        self,
        event_name: str,
        handler: Callable,
        plugin_name: str = "__system__",
        priority: EventPriority = EventPriority.NORMAL,
        filters: Optional[dict] = None,
    ):
        """Subscribe to an event."""
        eh = EventHandler(
            plugin_name=plugin_name,
            event_name=event_name,
            handler=handler,
            priority=priority,
            filters=filters,
        )
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(eh)

    def unsubscribe(self, event_name: str, plugin_name: str):
        """Unsubscribe plugin from event."""
        if event_name in self._event_handlers:
            self._event_handlers[event_name] = [
                h for h in self._event_handlers[event_name]
                if h.plugin_name != plugin_name
            ]

    # === Hook System ===

    def apply_hook(self, hook_name: str, data: Any) -> Any:
        """
        Apply a hook chain. Each handler can transform the data.

        Args:
            hook_name: Hook name
            data: Data to transform

        Returns:
            Transformed data
        """
        handlers = self._hook_handlers.get(hook_name, [])
        handlers.sort(key=lambda h: h.priority.value)

        for handler in handlers:
            info = self._plugins.get(handler.plugin_name)
            if not info or info.state != PluginState.STARTED:
                continue

            try:
                result = handler.handler(data)
                if result is not None:
                    data = result
                info.hook_count += 1
            except Exception as e:
                self._emit_log("hook_error", {
                    "hook": hook_name,
                    "plugin": handler.plugin_name,
                    "error": str(e),
                })

        return data

    def register_hook(
        self,
        hook_name: str,
        handler: Callable,
        plugin_name: str = "__system__",
        priority: HookPriority = HookPriority.NORMAL,
    ):
        """Register a hook handler."""
        hh = HookHandler(
            plugin_name=plugin_name,
            hook_name=hook_name,
            handler=handler,
            priority=priority,
        )
        if hook_name not in self._hook_handlers:
            self._hook_handlers[hook_name] = []
        self._hook_handlers[hook_name].append(hh)

    # === Query Methods ===

    def get_plugin(self, name: str) -> Optional[PluginInfo]:
        """Get plugin info by name."""
        return self._plugins.get(name)

    def get_plugin_instance(self, name: str) -> Optional[PluginBase]:
        """Get plugin instance by name."""
        info = self._plugins.get(name)
        return info.instance if info else None

    def list_events(self) -> dict[str, int]:
        """List registered events and handler counts."""
        return {
            event: len(handlers)
            for event, handlers in self._event_handlers.items()
        }

    def list_hooks(self) -> dict[str, int]:
        """List registered hooks and handler counts."""
        return {
            hook: len(handlers)
            for hook, handlers in self._hook_handlers.items()
        }

    def get_health(self) -> dict:
        """Get health status of all plugins."""
        return {
            "total": len(self._plugins),
            "active": len(self.active_plugins),
            "errors": sum(
                1 for p in self._plugins.values()
                if p.state == PluginState.ERROR
            ),
            "plugins": {
                name: info.to_dict() for name, info in self._plugins.items()
            },
            "events_registered": sum(
                len(h) for h in self._event_handlers.values()
            ),
            "hooks_registered": sum(
                len(h) for h in self._hook_handlers.values()
            ),
        }

    def get_event_log(self, limit: int = 50) -> list[dict]:
        """Get recent event log entries."""
        return self._event_log[-limit:]

    # === Internal Methods ===

    def _discover_handlers(self, plugin_name: str, instance: PluginBase):
        """Discover decorated event/hook handlers on a plugin instance."""
        for attr_name in dir(instance):
            try:
                attr = getattr(instance, attr_name)
            except Exception:
                continue

            if not callable(attr):
                continue

            # Event handlers
            if hasattr(attr, "_event_name"):
                eh = EventHandler(
                    plugin_name=plugin_name,
                    event_name=attr._event_name,
                    handler=attr,
                    priority=getattr(attr, "_event_priority", EventPriority.NORMAL),
                    filters=getattr(attr, "_event_filters", None),
                )
                event_name = attr._event_name
                if event_name not in self._event_handlers:
                    self._event_handlers[event_name] = []
                self._event_handlers[event_name].append(eh)

            # Hook handlers
            if hasattr(attr, "_hook_name"):
                hh = HookHandler(
                    plugin_name=plugin_name,
                    hook_name=attr._hook_name,
                    handler=attr,
                    priority=getattr(attr, "_hook_priority", HookPriority.NORMAL),
                )
                hook_name = attr._hook_name
                if hook_name not in self._hook_handlers:
                    self._hook_handlers[hook_name] = []
                self._hook_handlers[hook_name].append(hh)

        # Also register manually added handlers
        for event_name, handler, priority in instance._event_handlers:
            self.subscribe(event_name, handler, plugin_name, priority)
        for hook_name, handler, priority in instance._hook_handlers:
            self.register_hook(hook_name, handler, plugin_name, priority)

    def _remove_handlers(self, plugin_name: str):
        """Remove all handlers for a plugin."""
        for event_name in list(self._event_handlers.keys()):
            self._event_handlers[event_name] = [
                h for h in self._event_handlers[event_name]
                if h.plugin_name != plugin_name
            ]

        for hook_name in list(self._hook_handlers.keys()):
            self._hook_handlers[hook_name] = [
                h for h in self._hook_handlers[hook_name]
                if h.plugin_name != plugin_name
            ]

    def _emit_log(self, log_type: str, data: dict):
        """Add to internal event log."""
        entry = {"type": log_type, "timestamp": time.time(), **data}
        self._event_log.append(entry)
        if len(self._event_log) > self._max_log_size:
            self._event_log = self._event_log[-self._max_log_size:]
