import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

_ROUTES_ATTR = "__pinned_routes__"

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class RouteSpec:
    method: str
    path: str
    kwargs: dict[str, Any] = field(default_factory=dict)


def _register(method: str):
    def with_path(path: str, **kwargs: Any) -> Callable[[F], F]:
        if not path.startswith("/"):
            raise ValueError(f"route path must start with '/', got {path!r}")

        def decorator(fn: F) -> F:
            if not inspect.iscoroutinefunction(fn):
                raise TypeError(
                    f"pinned handler {fn.__qualname__!r} must be 'async def'; "
                    "sync handlers are unsupported (threadpool offload would break "
                    "single-threaded atomicity)"
                )
            specs = list(getattr(fn, _ROUTES_ATTR, []))
            specs.append(RouteSpec(method, path, kwargs))
            setattr(fn, _ROUTES_ATTR, specs)
            return fn

        return decorator

    return with_path


class route:
    get = staticmethod(_register("GET"))
    post = staticmethod(_register("POST"))
    put = staticmethod(_register("PUT"))
    delete = staticmethod(_register("DELETE"))
    patch = staticmethod(_register("PATCH"))


def routes_of(fn: Any) -> list[RouteSpec]:
    return list(getattr(fn, _ROUTES_ATTR, []))
