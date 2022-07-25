# OT/IT with Semantic Web Technologies (SWT)
This repository provides experimental tools for moving between the world of [Operational Technology (OT)](https://en.wikipedia.org/wiki/Operational_technology) to the world of Information Technology (IT).  

- __Model Mapper:__ - An implementation of [stOttr](https://dev.spec.ottr.xyz/stOTTR/) with extensions for mapping asset structures based on the [Epsilon Transformation Language](https://www.eclipse.org/epsilon/doc/etl/). Implemented with [Apache Arrow](https://arrow.apache.org/) in Rust using [Pola.rs](https://www.pola.rs/).  
- __Hybrid Query Engine:__ [SPARQL](https://www.w3.org/TR/sparql11-overview/)- and [Apache Arrow](https://arrow.apache.org/)-based high throughput access to time series data residing in an arbitrary time series database which is contextualized by a knowledge graph. Built in [Rust](https://www.rust-lang.org/) using [pola.rs](https://www.pola.rs/), [spargebra](https://docs.rs/spargebra/latest/spargebra/), [sparesults](https://docs.rs/sparesults/0.1.1/sparesults/) and [oxrdf](https://docs.rs/oxrdf/latest/oxrdf/) from the [Oxigraph](https://github.com/oxigraph/oxigraph) project.  
- __Domain Specific Query Language:__ A customizable query language for accessing time series data using simple generalized paths such as those found in the [Reference Designation System](https://www.iso.org/standard/82229.html) or in [OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/) information models. The DSQL is parsed with [nom](https://docs.rs/nom/latest/nom/) and translated to the Hybrid Query language.

Currently, these tools are volatile works in progress, and should not be used by anyone for anything important. 

## Queries
```python
  from otit_swt_query import Engine, ArrowFlightSQLDatabase, TimeSeriesTable
  SPARQL_QUERY_ENDPOINT = "http://127.0.0.1:7878/query"
  DREMIO_HOST = "127.0.0.1"
  DREMIO_PORT = 32010

  engine = Engine(SPARQL_QUERY_ENDPOINT)
  tables = [TimeSeriesTable(
      schema="my_schema", time_series_table="my_table",
      value_column="values", timestamp_column="timestamps", 
      identifier_column="identifiers",
      value_datatype="http://www.w3.org/2001/XMLSchema#unsignedInt")]
  arrow_flight_sql_database = ArrowFlightSQLDatabase(
      host=DREMIO_HOST, port=DREMIO_PORT, 
      username="dremio", password="dremio123", tables=tables)
  engine.arrow_flight_sql(arrow_flight_sql_database)
  
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
    FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .}
""")
```


All code is licensed to [Prediktor AS](https://www.prediktor.com/) under the Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree.  