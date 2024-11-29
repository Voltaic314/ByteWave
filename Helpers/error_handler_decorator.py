import functools
import traceback


def error_handler(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            print(f"Error in {func.__name__}: {traceback.format_exc()}")
            return {"error": str(e)}
    return wrapper
