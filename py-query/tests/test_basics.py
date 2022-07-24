import pathlib

from otit_swt_query import Engine, ArrowFlightSQLDatabase, TimeSeriesTable
import polars as pl

OXIGRAPH_QUERY_ENDPOINT = "http://127.0.0.1:7878/query"
DREMIO_HOST = "127.0.0.1"
DREMIO_PORT = 32010
PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


def test_simple_query(dremio_testdata, oxigraph_testdata):
    engine = Engine(OXIGRAPH_QUERY_ENDPOINT)
    tables = [
        TimeSeriesTable(
            schema="my_nas",
            time_series_table="ts.parquet",
            value_column="v",
            timestamp_column="ts",
            identifier_column="id",
            value_datatype="http://www.w3.org/2001/XMLSchema#unsignedInt")
    ]
    arrow_flight_sql_database = ArrowFlightSQLDatabase(host=DREMIO_HOST, port=DREMIO_PORT, username="dremio",
                                                       password="dremio123", tables=tables)
    engine.arrow_flight_sql(arrow_flight_sql_database)
    df = engine.execute_hybrid_query("""
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX quarry:<https://github.com/magbak/quarry-rs#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s quarry:hasTimeseries ?ts .
        ?ts quarry:hasDataPoint ?dp .
        ?dp quarry:hasTimestamp ?t .
        ?dp quarry:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    """)
    expected_csv = TESTDATA_PATH / "expected_simple_query.csv"
    expected_df = pl.read_csv(expected_csv, parse_dates=True)
    pl.testing.assert_frame_equal(df, expected_df, check_dtype=False)
