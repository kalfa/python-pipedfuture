"""Callback classes"""

from functools import partial
from concurrent import futures

class PipedFuture(futures.Future):
    """Add pipelined callback to Futures.

    In addition to normal futures.Future functionalities, PipedFuture allows to
    define callbacks which are fed by the previous result.
    Like in a pipeline, the first callback is fed by the triggering event's
    result, any further callback will be fed by the result produced by the
    previous one.
    The last callback result will be the final result of the pipeline.

    Thereof the registration order of the callbacks is important and will
    influence the final Future result.
    """
    def __init__(self, desc=None):
        futures.Future.__init__(self)
        self._result = None
        self.desc = desc

    def add_done_callback(self, fn):
        """A different queue of callbacks.

        Each callback result is passed to the next one, the last callback sets
        the Future's result.
        If a callback is added later in time, the Future's result is updated
        with the last callback result.
        """
        def update_result_wrapper(fn, future):
            """Execute a callback and set its result as the current one"""
            result = fn(future)
            with self._condition:
                self._result = result
                self._condition.notify_all()

        # Wrapping the done callbck, we ensure that when executred it will also
        # set its result as Future's result.
        futures.Future.add_done_callback(self,
                                         partial(update_result_wrapper, fn))

    def add_done_future(self, future):
        """Add a future as a callback

        The PipedFuture instance will act as Executor of the passed future.
        If passed future instance is cancelled, it will be skipped in the
        pipeline.

        The future result will be passed to the next callback.

        @param future a Future instance, being it PipedFuture or futures.Future

        """
        def propagate_result(to_future, from_future):
            """Propagate the current instance result to the added future

            If the future is cancelled, pass on 'from_future' result, otherwise
            run to_future and pass on its result to the pipeline.
            """
            if to_future.set_running_or_notify_cancel():
                to_future.set_result(from_future.result())
                result = to_future.result()
            else:
                result = from_future.result()
            return result

        self.add_done_callback(partial(propagate_result, future))
