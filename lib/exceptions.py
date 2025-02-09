import traceback


__all__ = (
    'AsyncException', 'VideoError',
)


class AsyncException(Exception):
    """  Prettify-able, self-logging async I/O exception

        ** Example usage **

        try:
          raise ValueError('not a key')
        except Exception as e:
          err = AsyncException(f'Error scraping video "{1234}"', )
          print(e.__dict__)
    """

    def __init__(self, message, exc=None):
        exc = exc or self
        self.message = f"🚫 async error: {message}"
        self.detail = str(exc)
        self.errors = ''.join(traceback.format_exception(exc))

        super().__init__(message)


class VideoError(AsyncException):
    def __init__(self, video_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.video_id = video_id
