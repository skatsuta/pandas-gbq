"""Helper methods for loading data into BigQuery"""

import io

from google.cloud import bigquery

import pandas_gbq.schema


def encode_chunk(dataframe, source_format=bigquery.SourceFormat.CSV):
    """Return a file-like object of JSON- or CSV-encoded rows.

    Args:
      dataframe (pandas.DataFrame): A chunk of a dataframe to encode
    """

    if source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON:
        encoded = dataframe.to_json(
            orient="records",
            date_format="iso",
            double_precision=17,
            lines=True,
        )
    else:
        encoded = dataframe.to_csv(
            index=False,
            header=False,
            encoding="utf-8",
            float_format="%.17g",
            date_format="%Y-%m-%d %H:%M:%S.%f",
        )

    # Convert to a BytesIO buffer so that unicode text is properly handled.
    # See: https://github.com/pydata/pandas-gbq/issues/106
    return io.BytesIO(encoded.encode("utf-8"))


def encode_chunks(
    dataframe, chunksize=None, source_format=bigquery.SourceFormat.CSV
):
    dataframe = dataframe.reset_index(drop=True)
    if chunksize is None:
        yield 0, encode_chunk(dataframe, source_format=source_format)
        return

    remaining_rows = len(dataframe)
    total_rows = remaining_rows
    start_index = 0
    while start_index < total_rows:
        end_index = start_index + chunksize
        chunk_buffer = encode_chunk(
            dataframe[start_index:end_index], source_format=source_format
        )
        start_index += chunksize
        remaining_rows = max(0, remaining_rows - chunksize)
        yield remaining_rows, chunk_buffer


def load_chunks(
    client,
    dataframe,
    destination_table_ref,
    chunksize=None,
    schema=None,
    location=None,
):
    if schema is None:
        schema = pandas_gbq.schema.generate_bq_schema(dataframe)
    schema = pandas_gbq.schema.add_default_nullable_mode(schema)

    fields = [
        bigquery.SchemaField.from_api_repr(field) for field in schema["fields"]
    ]
    source_format = _determine_source_format(fields)
    job_config = bigquery.LoadJobConfig(
        allow_quoted_newlines=True,
        schema=fields,
        source_format=source_format,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    new_df = _object_to_string(dataframe, fields)
    chunks = encode_chunks(
        new_df, chunksize=chunksize, source_format=source_format
    )
    for remaining_rows, chunk_buffer in chunks:
        try:
            yield remaining_rows
            client.load_table_from_file(
                chunk_buffer,
                destination_table_ref,
                job_config=job_config,
                location=location,
            ).result()
        finally:
            chunk_buffer.close()


def _determine_source_format(fields):
    if any(_is_complex_type(field) for field in fields):
        return bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    return bigquery.SourceFormat.CSV


def _is_complex_type(field):
    return field.field_type == "RECORD" or field.mode == "REPEATED"


def _object_to_string(dataframe, fields):
    new_df = dataframe.copy()
    complex_fields = {
        field
        for field in fields
        if field.name in new_df and _is_complex_type(field)
    }
    obj_to_str_cols = [
        col
        for col in new_df.select_dtypes("object")
        if col not in complex_fields
    ]
    new_df[obj_to_str_cols] = new_df[obj_to_str_cols].astype("string")
    return new_df
