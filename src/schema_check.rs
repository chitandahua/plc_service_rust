use anyhow::Context;
use jsonschema::{Draft, JSONSchema};
use std::path::PathBuf;

use crate::Result;

pub fn parse_schema(filepath: PathBuf) -> Result<JSONSchema> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(filepath)?;
    let reader = BufReader::new(file);
    let schema_value = serde_json::from_reader(reader)?;

    JSONSchema::options()
        .with_draft(Draft::Draft7)
        .compile(&schema_value)
        .map_err(|validation| anyhow::anyhow!("{validation}"))
}

pub fn schema_check(schema: &JSONSchema, payload: &str) -> Result<()> {
    let payload: serde_json::Value =
        serde_json::from_str(payload).context("parse payload failed")?;

    if let Err(errors) = schema.validate(&payload) {
        for error in errors {
            //info!("Validation error: {}", error);
            //info!("Instance path: {}", error.instance_path);

            return Err(anyhow::anyhow!(error.to_string()));
        }
    }

    Ok(())
}
