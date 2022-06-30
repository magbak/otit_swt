use nom::Finish;
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use polars::series::Series;
use mapper::mapping::Mapping;
use mapper::parser::stottr_doc;
use mapper::resolver::resolve_document;
use mapper::templates::TemplateDataset;

#[test]
fn test_mapper_easy_case() {
    let t_str = r#"
    @prefix ex:<http://example.net/ns#>.

    ex:ExampleTemplate [?myVar1 , ?myVar2]
      :: {
        ottr:Triple(ex:anObject, ex:hasNumber, ?myVar1) ,
        ottr:Triple(ex:anObject, ex:hasOtherNumber, ?myVar2)
      } .
    "#;
    let (_,doc) = stottr_doc(t_str).finish().expect("Ok");
    let doc = resolve_document(doc).expect("Resolution problem");
    let template_dataset = TemplateDataset::new(vec![doc]).expect("Dataset problem");

    let mut k = Series::from_iter(["KeyOne", "KeyTwo"]);
    k.rename("Key");
    let mut v1 = Series::from_iter(&[1, 2i32]);
    v1.rename("myVar1");
    let mut v2 = Series::from_iter(&[3,4i32]);
    v2.rename("myVar2");
    let series = [k, v1, v2];
    let df = DataFrame::from_iter(series);


    let mut mapping = Mapping::new(&template_dataset);
    let report = mapping.expand(&NamedNode::new_unchecked("http://example.net/ns#ExampleTemplate"), df);

}