import pathlib

import pytest
from SPARQLWrapper import SPARQLWrapper, POST, JSON

from otit_swt_query import Engine, ArrowFlightSQLDatabase, TimeSeriesTable
import polars as pl

OXIGRAPH_UPDATE_ENDPOINT = "http://127.0.0.1:7878/update"
OXIGRAPH_QUERY_ENDPOINT = "http://127.0.0.1:7878/query"
DREMIO_HOST = "127.0.0.1"
DREMIO_PORT = 32010
PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

@pytest.fixture(scope="module")
def oxigraph_testdata(oxigraph_db):
    ep = SPARQLWrapper(OXIGRAPH_UPDATE_ENDPOINT)
    with open(PATH_HERE / "testdata" / "testdata_arrow_flight_sql.sparql") as f:
        query = f.read()
    ep.setMethod(POST)
    ep.setReturnFormat(JSON)
    ep.setQuery(query)
    res = ep.query()
    print(res)

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
    engine.set_arrow_flight_sql(arrow_flight_sql_database)
    df = engine.execute_hybrid_query("""
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s otit_swt:hasTimeseries ?ts .
        ?ts otit_swt:hasDataPoint ?dp .
        ?dp otit_swt:hasTimestamp ?t .
        ?dp otit_swt:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    """)
    expected_csv = TESTDATA_PATH / "expected_simple_query.csv"
    expected_df = pl.read_csv(expected_csv, parse_dates=True)
    pl.testing.assert_frame_equal(df, expected_df, check_dtype=False)


def test_dsl_query(dremio_testdata, oxigraph_testdata):
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
    engine.set_arrow_flight_sql(arrow_flight_sql_database)

    engine.name_predicate("http://www.w3.org/2000/01/rdf-schema#label")
    engine.connective_mapping({".":"http://example.org/types#hasComponent"})

    df = engine.execute_dsl_query("""
    BigWidget."ImportantSensor"
    from 2022-06-01T08:46:53+00:00
    to 2023-12-02T00:00:01+00:00
    aggregate mean 5s
    """)
    df = df.sort(["BigWidget_ImportantSensor__path_name", "timestamp"])

    expected_csv = TESTDATA_PATH / "expected_dsl_query.csv"
    #df.to_csv(expected_csv)
    expected_df = pl.read_csv(expected_csv, parse_dates=True)
    expected_df = expected_df.sort(["BigWidget_ImportantSensor__path_name", "timestamp"])
    pl.testing.assert_frame_equal(df, expected_df, check_dtype=False)
