from services.stream_gateway.app import to_frame


def test_rows_become_frames_unchanged():
    row = {"seq": 4, "kind": "page_result",
           "envelope": {"header": {"seq": 4, "page_index": 2}, "payload": {"page_index": 2}}}
    frame = to_frame(row)
    assert (frame.seq, frame.kind, frame.data) == (4, "page_result", row["envelope"])
    assert frame.sse().startswith("id: 4\nevent: page_result\ndata: {")
    assert not frame.final and to_frame(dict(row, kind="job_complete")).final
