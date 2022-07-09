import polars as pl
from otit_swt_mapper import Mapping

def test_create_mapping_from_polars_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
  } .
"""

    df = pl.DataFrame({"Key": ["A", "B"],
                       "MyValue": [1, 2]})
    #df.rechunk()
    mapping = Mapping([doc])
    #batches = df.to_arrow().to_batches(99999999);
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
