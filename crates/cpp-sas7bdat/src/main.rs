use std::time::Instant;
use polars::frame::DataFrame;
use cpp_sas7bdat::to_polars::{
    get_sas_polars_schema,
    SasDataFrameIterator
};
fn main() {
    let start = Instant::now();
    // let path = "/home/jrothbaum/Coding/polars_readstat/crates/readstat-tests/tests/data/pyreadstat/basic/sample.sas7bdat";
    let path = "/home/jrothbaum/Downloads/sas_pil/psam_p17.sas7bdat";
    let schema = get_sas_polars_schema(&path);
    println!("Schema: {:?}", schema.unwrap());
    let mut iter = SasDataFrameIterator::new(
        &path,
        20_000 as usize,
    ).unwrap();
    
    let mut i_rows: usize = 0;
    for chunk_result in iter {
        // Call the method on the iterator
        let df = match chunk_result {
            Ok(df) => {
                println!("DataFrame shape: {:?}", df.shape());
                println!("DataFrame shape: {:?}", df.estimated_size());
                
                // println!("{:?}", df);
                i_rows = i_rows + df.height();
                df
            },
            Err(e) => {
                print!("Polars error: {}",e);
                DataFrame::empty()
            }
        };
    }
    let duration = start.elapsed();
    println!("Function took: {:?}", duration);
    println!("Rows: {:?}", i_rows);
}