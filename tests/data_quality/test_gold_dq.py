def test_gold_schema(df_gold, gold_schema):
    # Check required columns
    for col, spec in gold_schema["columns"].items():
        assert col in df_gold.columns
        if spec.get("nullable") is False:
            assert df_gold[col].notnull().all()
        if "type" in spec:
            assert df_gold[col].dtype.name == spec["type"]


def test_primary_keys_unique(df_gold, gold_schema):
    pk_cols = [
        col for col, spec in gold_schema["columns"].items() if spec.get("primary_key")
    ]
    assert not df_gold.duplicated(subset=pk_cols).any()
