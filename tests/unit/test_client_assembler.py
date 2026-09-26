from client.cli import Assembler


def frame(seq, kind, page=None, total=4, payload=None):
    return {"header": {"seq": seq, "page_index": page, "total_pages": total, "watermark": 0},
            "payload": payload if payload is not None else {"page_index": page}}


def test_out_of_order_pages_flush_in_document_order():
    asm = Assembler()
    asm.accept("job_split", frame(1, "job_split", payload={"total_pages": 4}))
    assert asm.accept("page_result", frame(2, "page_result", 2)) == []
    assert asm.accept("page_result", frame(3, "page_result", 1)) == []
    flushed = asm.accept("page_result", frame(4, "page_result", 0))
    assert [p["page_index"] for p in flushed] == [0, 1, 2]
    assert asm.accept("page_result", frame(4, "page_result", 0)) == []  # duplicate seq
    asm.accept("page_fallback", frame(5, "page_fallback", 3))
    asm.accept("job_complete", frame(6, "job_complete", payload={}))
    assert asm.complete and [p["page_index"] for p in asm.ordered] == [0, 1, 2, 3]
    assert asm.duplicates == 1 and asm.out_of_order == 2 and asm.max_buffered == 3
