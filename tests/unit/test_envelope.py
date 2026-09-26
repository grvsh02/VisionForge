from vf_common import envelope


def test_watermark_is_first_gap():
    assert envelope.watermark(set(), 10) == 0
    assert envelope.watermark({0, 1, 2, 5}, 10) == 3
    assert envelope.watermark(set(range(10)), 10) == 10


def test_window_bitmap_round_trip():
    done = {0, 1, 3, 9, 65, 66}
    wm = envelope.watermark(done, 100)
    bitmap = envelope.window_bitmap(done, wm)
    assert envelope.decode_bitmap(bitmap, wm) == {3, 9, 65}  # 66 is outside [2, 66)


def test_header_fields():
    h = envelope.build_header(job_id="j", client_id="c", seq=7, page_index=4, total_pages=6,
                              done_pages=[0, 1, 4])
    assert h["seq"] == 7 and h["watermark"] == 2
    assert h["window"]["base"] == 2 and h["window"]["size"] == envelope.WINDOW_SIZE
    assert envelope.decode_bitmap(h["window"]["bitmap"], 2) == {4}
