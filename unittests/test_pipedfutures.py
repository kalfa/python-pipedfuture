from unittest import TestCase, mock
from concurrent import futures

from pipedfutures import PipedFuture

def inc(f):
    if not f.done():
        raise Exception('not done')
    return f.result() + 1


def inc2(f):
    if not f.done():
        raise Exception('not done')
    return f.result() + 2


class TestPipedFuture(TestCase):
    def test_set_result_can_be_called_multiple_times(self):
        # This is not a introduces feature, but needs to be true in order for
        # PipedFuture to work.
        expected_results = range(100)
        unit = PipedFuture()
        for i in expected_results:
            unit.set_result(i)
            self.assertEqual(unit.result(), i)

    def test_chained_callback_result_propagate_to_the_next_one(self):
        inc1 = mock.MagicMock()
        inc1.side_effect = inc

        unit = PipedFuture()
        unit.add_done_callback(inc1)
        unit.add_done_callback(inc1)

        unit.set_result(1)
        self.assertEqual(unit.result(), 3)
        inc1.assert_has_calls([mock.call(unit),
                               mock.call(unit)])

    def test_callback_added_later_updates_result(self):
        inc1 = mock.MagicMock()
        inc1.side_effect = inc

        unit = PipedFuture()
        unit.add_done_callback(inc1)
        unit.set_result(1)
        self.assertEqual(unit.result(), 2)
        inc1.assert_has_calls([mock.call(unit)])

        unit.add_done_callback(inc1)
        self.assertEqual(unit.result(), 3)

        inc1.assert_has_calls([mock.call(unit),
                               mock.call(unit)])

    def test_pipedfutures_added_is_triggered_and_pipelined(self):
        inc1 = mock.MagicMock()
        inc1.side_effect = inc

        embedded = PipedFuture()
        embedded.add_done_callback(inc1)

        unit = PipedFuture()
        unit.add_done_callback(inc1)
        unit.add_done_future(embedded)
        unit.add_done_callback(inc1)

        unit.set_result(0)
        self.assertEqual(unit.result(), 3)
        # initial result is the pipeline result until that point, final result
        # is the final result of its pipeline
        self.assertEqual(embedded.result(), 2)

        inc1.assert_has_calls([mock.call(unit),
                               mock.call(embedded),
                               mock.call(unit)])

    def test_futures_added_is_triggered_and_pipelined(self):
        inc1 = mock.MagicMock()
        inc1.side_effect = inc

        embedded = futures.Future()
        embedded.add_done_callback(inc1)

        unit = PipedFuture()
        unit.add_done_callback(inc1)
        unit.add_done_future(embedded)
        unit.add_done_callback(inc1)

        unit.set_result(0)
        self.assertEqual(unit.result(), 2)
        # initial result is the pipeline result until that point, final result
        # is still its initial result since it's a standard Future instance.
        self.assertEqual(embedded.result(), 1)

        inc1.assert_has_calls([mock.call(unit),
                               mock.call(embedded),
                               mock.call(unit)])

    def test_result_on_unset_future(self):
        inc1 = mock.MagicMock()
        inc1.side_effect = inc

        unit = PipedFuture()
        unit._condition = mock.MagicMock()
        unit.add_done_callback(inc1)

        self.assertRaises(futures.TimeoutError, unit.result, 1)
