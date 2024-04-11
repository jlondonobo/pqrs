
use clap::{Parser, Subcommand};

use crate::errors::PQRSError;
use std::fs::File;
mod commands;
mod errors;
mod utils;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, DataType, Schema};
use std::sync::Arc;
use base64::prelude::*;

#[derive(Subcommand, Debug)]
enum Commands {
    Cat(commands::cat::CatCommandArgs),
    Head(commands::head::HeadCommandArgs),
    Merge(commands::merge::MergeCommandArgs),
    #[command(alias = "rowcount")]
    RowCount(commands::rowcount::RowCountCommandArgs),
    Sample(commands::sample::SampleCommandArgs),
    Schema(commands::schema::SchemaCommandArgs),
    Size(commands::size::SizeCommandArgs),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Show debug output
    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    command: Commands,
}

fn main() -> Result<(), PQRSError> {
    let file_name = "/Users/joselondono/Downloads/geodataframe.parquet";
    let file = File::open(file_name)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.with_limit(10).build().unwrap();

    let mut writer = arrow::csv::Writer::new(std::io::stdout());

    let record_batch = reader.next().unwrap().unwrap();


    let schema = record_batch.schema();
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Binary | DataType::LargeBinary => {
                let binary_column = record_batch.column(i);
                let string_column = binary_column
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|maybe_binary| maybe_binary.map(|binary| BASE64_STANDARD.encode(binary)).unwrap_or_default())
                    .collect::<Vec<String>>();

                let string_column = arrow::array::StringArray::from_iter_values(string_column);

                fields.push(Arc::new(Field::new(field.name(), DataType::Utf8, field.is_nullable())));
                columns.push(Arc::new(string_column) as Arc<dyn arrow::array::Array>);
            }
            _ => {
                fields.push(field.clone());
                columns.push(record_batch.column(i).clone())
            }
            
        }
    }
    let processed_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    writer.write(&processed_batch)?;

    Ok(())

    // if args.debug {
    //     std::env::set_var("RUST_LOG", "debug");
    // }
    // env_logger::init();

    // log::debug!("args: {:?}", args);

    // match args.command {
    //     Commands::Cat(opts) => commands::cat::execute(opts)?,
    //     Commands::Head(opts) => commands::head::execute(opts)?,
    //     Commands::Merge(opts) => commands::merge::execute(opts)?,
    //     Commands::RowCount(opts) => commands::rowcount::execute(opts)?,
    //     Commands::Sample(opts) => commands::sample::execute(opts)?,
    //     Commands::Schema(opts) => commands::schema::execute(opts)?,
    //     Commands::Size(opts) => commands::size::execute(opts)?,
    // }

}
