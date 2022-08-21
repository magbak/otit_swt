# OT/IT SWT Mapper Documentation

The mapper implements the [stOttr](https://dev.spec.ottr.xyz/stOTTR/) language with extensions for mapping asset structures inspired by the [Epsilon Transformation Language](https://www.eclipse.org/epsilon/doc/etl/). 
Implemented with [Apache Arrow](https://arrow.apache.org/) in Rust using [Pola.rs](https://www.pola.rs/). 
We provide a Python wrapper for the library, which allows us to create mappings using DataFrames. 

## API
The API is simple, and contains only one class and a few methods.
```python
from otit_swt_mapper import Mapping, ResolveIRI, MintingOptions, to_graph
import polars as pl
```

Mapping-objects are initialized using a list of stOttr documents (each a string). 
```python
doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """
mapping = Mapping([doc])
```

In order to extend this template, we provide a DataFrame with a particular signature.
We use the _extend_-method of the mapping-object.

```python
from typing import Optional, Dict
def extend(self, 
           template: str,
           df: pl.DataFrame,
           resolve_iris: Optional[Dict[str, ResolveIRI]] = None,
           mint_iris: Optional[Dict[str, MintingOptions]] = None,
           language_tags: Optional[Dict[String, String]]=None
           ) -> Optional[pl.DataFrame]
```
The _template_-argument specifies the template we want to expand. 
We are allowed to use the prefixes in the stOttr documents when referring to these unless there are conflicting prefix-definitions. 
The _df_ provided must have a column called _Key_, which serves as a special bookkeeping column. 
The parameters of the templates must be provided as identically-named columns. To provide a null-argument, just make a column of nulls.

### Mint IRIs
The mint_iris-argument allows us to specify IRI-arguments that should be minted. 
We specify the column names we want to mint IRIs for and do not specify them in the _df_-argument.
The MintingOptions class describes options for IRI-creation. 
Currently we only support incrementing a number prefixed by a string.
```python
MintingOptions(
    prefix:str, 
    numbering_suffix_start:int)
```
When extend is run with _mint_iris_ specified, the minted iris are returned in a DataFrame together with the _Key_-column. 
When mapping assets one often has to specify the properties of equipment individually, possibly minting IRIs, and then linking these IRIs together to represent how equipment is linked in the industrial asset. 
The purpose of the returned DataFrame is to be able to use to these IRIs later in the mapping process as an argument to some other template.
Alternatively, the _resolve_iris_ argument can be used.

### Resolve IRIs
```python
ResolveIRI(
    key_column: str,
    template: str,
    argument: str
)
```
In the _resolve_iri_ argument we specify a dict of the columns where we want to resolve minted IRIs.
We must provide an additional column containing the Key associated with the minted IRIs we want to resolve, the template and argument that we want to resolve.

## Exporting
Multiple alternatives exist to export the mapped triples. The fastest way to serialize is the _write_ntriples_-method.
```python
mapping.write_ntriples(file:str)
```

Alternatively, we can export the mapping to an [rdflib](https://rdflib.readthedocs.io/en/stable/)-graph. 

```python
gr = to_graph(mapping)
```
