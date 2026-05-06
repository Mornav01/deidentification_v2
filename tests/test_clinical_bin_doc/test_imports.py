def test_package_importable():
    import deid.clinical_bin_doc
    assert hasattr(deid.clinical_bin_doc, "__name__")
