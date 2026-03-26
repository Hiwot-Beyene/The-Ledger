import pytest

from src.models import BBox, LDU, PageIndexNode, ProvenanceChain


def test_bbox_validator_rejects_invalid_order():
    with pytest.raises(ValueError):
        BBox(x0=10, y0=20, x1=5, y1=25)


def test_ldu_rejects_invalid_page_refs():
    with pytest.raises(ValueError):
        LDU(
            id="ldu-1",
            text="hello",
            content_hash="12345678",
            page_refs=[0],
            provenance_chain=[],
        )


def test_recursive_page_index_node_builds_with_children():
    leaf = PageIndexNode(id="n2", node_type="text_block", page_number=1)
    root = PageIndexNode(id="n1", node_type="page",
                         page_number=1, children=[leaf])
    assert len(root.children) == 1
    assert root.children[0].id == "n2"


def test_provenance_chain_has_required_fields():
    chain = ProvenanceChain(
        document_name="sample.pdf",
        page_number=1,
        bbox=BBox(x0=0, y0=0, x1=1, y1=1),
        content_hash="abcdef12",
    )
    assert chain.document_name == "sample.pdf"
