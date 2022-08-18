import asyncio
import os
import pathlib
import time
from multiprocessing import Process
import polars as pl

import pytest
from SPARQLWrapper import SPARQLWrapper, POST, JSON
from asyncua import Server, ua
from asyncua.server.history_sql import HistorySQLite
from asyncua.ua import NodeId, String, Int16, DataValue, Variant
from datetime import datetime

from otit_swt_query import Engine, OPCUAHistoryRead, TimeSeriesTable
from py_otit_swt_query.tests.conftest import OXIGRAPH_UPDATE_ENDPOINT

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"
HISTORY_DB_PATH = PATH_HERE / "history.db"

OXIGRAPH_QUERY_ENDPOINT = "http://127.0.0.1:7878/query"
OPCUA_ENDPOINT = "opc.tcp://127.0.0.1:4841/freeopcua/server/"

async def start_opcua_server() -> Server:
    # setup our server
    server = Server()

    db = HistorySQLite(str(HISTORY_DB_PATH))
    # Configure server to use sqlite as history database (default is a simple memory dict)
    server.iserver.history_manager.set_storage(db)

    # initialize server
    await server.init()

    server.set_endpoint("opc.tcp://0.0.0.0:4841/freeopcua/server/")
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity])

    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = await server.register_namespace(uri)
    print("Namespace index: ", idx)

    # get Objects node, this is where we should put our custom stuff
    objects = server.nodes.objects

    # populating our address space
    var1 = await objects.add_variable(NodeId("ts1", idx), "Timeseries1", 0)
    var2 = await objects.add_variable(NodeId("ts2", idx), "Timeseries2", 0)

    # starting!
    await server.start()

    # enable data change history for this particular node, must be called after start since it uses subscription
    await server.historize_node_data_change(var1, period=None, count=0)
    await server.historize_node_data_change(var2, period=None, count=0)

    for c in range(60):
        await var1.write_value(DataValue(Value=Variant(100 + c), SourceTimestamp=datetime(2022, 8, 17, 10, 42, c)))
        await var2.write_value(DataValue(Value=Variant(200 + c), SourceTimestamp=datetime(2022, 8, 17, 10, 42, c)))

    #Necessary for the server to stay alive
    await asyncio.sleep(1000)


#Based on example from https://github.com/FreeOpcUa/opcua-asyncio/blob/master/examples/server-datavalue-history.py
@pytest.fixture
def opcua_server():
    if os.path.exists(HISTORY_DB_PATH):
        os.remove(HISTORY_DB_PATH)
    p = Process(
        target=asyncio.run, args=(start_opcua_server(),), daemon=True)
    p.start()
    time.sleep(2)


@pytest.fixture(scope="session")
def oxigraph_testdata(oxigraph_db):
    ep = SPARQLWrapper(OXIGRAPH_UPDATE_ENDPOINT)
    with open(PATH_HERE / "testdata" / "testdata_opcua_history_read.sparql") as f:
        query = f.read()
    ep.setMethod(POST)
    ep.setReturnFormat(JSON)
    ep.setQuery(query)
    res = ep.query()
    #print(res)

def test_simplified_opcua_case(opcua_server, oxigraph_testdata):
    print("Begin test")
    engine = Engine(OXIGRAPH_QUERY_ENDPOINT)
    print("defined engine")
    opcua_backend = OPCUAHistoryRead(namespace=2, endpoint=OPCUA_ENDPOINT)
    print("created opcua backend")
    engine.set_opcua_history_read(opcua_backend)
    print("Set backend")
    df = engine.execute_hybrid_query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX otit_swt:<https://github.com/magbak/otit_swt#>
        PREFIX types:<http://example.org/types#>
        SELECT ?w ?s ?mytype ?t ?v WHERE {
            ?w a ?mytype .
            ?w types:hasSensor ?s .
            ?s otit_swt:hasTimeseries ?ts .
            ?ts otit_swt:hasDataPoint ?dp .
            ?dp otit_swt:hasTimestamp ?t .
            ?dp otit_swt:hasValue ?v .
            FILTER(?t < "2022-08-17T16:46:53"^^xsd:dateTime && ?v > 150.0) .
        }
        """)
    expected_csv = TESTDATA_PATH / "expected_simplified_opcua_case.csv"
    df.to_csv(expected_csv)
    expected_df = pl.read_csv(expected_csv, parse_dates=True)
    pl.testing.assert_frame_equal(df, expected_df, check_dtype=False)
