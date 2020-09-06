import json
import typer
from typer import Option
import apache_beam as beam


def main(
    project: str = Option(..., help='Your Google Cloud Project'),
    topic: str = Option(..., help='Your PubSub Topic'),
    bucket: str = Option(..., help='Your tmp/test Input GCS'),
    dataset: str = Option(..., help='Your Output BigQuery Dataset'),
    table: str = Option(..., help='Your Output BigQuery Table'),
    runner: bool = Option(..., '--DirectRunner/--DataflowRunner',
                          help='Your Pipeline Runner'),
):
    '''
    Awesome Pipeline
    '''
    if runner:
        beam_runner = 'DirectRunner'
    else:
        beam_runner = 'DataflowRunner'

    settings = [
        f'--project={project}',
        '--job_name=job1',
        f'--staging_location=gs://{bucket}/staging/',
        f'--temp_location=gs://{bucket}/tmp/',
        f'--runner={beam_runner}',
        '--region=asia-northeast1',
        '--max_num_workers=1',
    ]

    if not runner:
        settings.append('--streaming')

    p = beam.Pipeline(argv=settings)

    if runner:
        pcoll = p | 'ReadFrom' >> beam.io.ReadFromText(f'gs://{bucket}/test/test.json')
    else:
        pcoll = p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            topic=f'projects/{project}/topics/{topic}')

    (pcoll
        | 'LoadJSON' >> beam.Map(json.loads)
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            project=project,
            dataset=dataset,
            table=table,
            schema='id:INTEGER,name:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
     )

    if runner:
        p.run().wait_until_finish()
    else:
        p.run()


if __name__ == "__main__":
    typer.run(main)
